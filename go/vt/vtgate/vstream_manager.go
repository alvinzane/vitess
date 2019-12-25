/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

// vstreamManager manages vstream requests.
type vstreamManager struct {
	resolver *srvtopo.Resolver
	toposerv srvtopo.Server
	cell     string
}

// vstream contains the metadata for one VStream request.
type vstream struct {
	// mu protects vgtid and err
	mu    sync.Mutex
	vgtid *binlogdatapb.VGtid
	err   error

	// Other input parameters
	tabletType topodatapb.TabletType
	filter     *binlogdatapb.Filter
	send       func(events []*binlogdatapb.VEvent) error
	resolver   *srvtopo.Resolver
}

func newVStreamManager(resolver *srvtopo.Resolver, serv srvtopo.Server, cell string) *vstreamManager {
	return &vstreamManager{
		resolver: resolver,
		toposerv: serv,
		cell:     cell,
	}
}

func (vsm *vstreamManager) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, send func(events []*binlogdatapb.VEvent) error) error {
	filter, vgtid, err := vsm.resolveParams(ctx, tabletType, vgtid, filter)
	if err != nil {
		return err
	}
	vs := &vstream{
		vgtid:      vgtid,
		tabletType: tabletType,
		filter:     filter,
		send:       send,
		resolver:   vsm.resolver,
	}
	return vs.stream(ctx)
}

// resolveParams provides defaults for the inputs if they're not specified.
func (vsm *vstreamManager) resolveParams(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter) (*binlogdatapb.Filter, *binlogdatapb.VGtid, error) {
	if filter == nil {
		filter = &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*",
			}},
		}
	}
	if vgtid == nil {
		vgtid = &binlogdatapb.VGtid{}
	}
	if len(vgtid.ShardGtids) == 0 {
		keyspaces, err := vsm.toposerv.GetSrvKeyspaceNames(ctx, vsm.cell)
		if err != nil {
			return nil, nil, err
		}
		for _, keyspace := range keyspaces {
			vgtid.ShardGtids = append(vgtid.ShardGtids, &binlogdatapb.ShardGtid{Keyspace: keyspace})
		}
	}
	newvgtid := &binlogdatapb.VGtid{}
	for _, sgtid := range vgtid.ShardGtids {
		if sgtid.Shard == "" {
			// TODO(sougou): this should work with the new Migrate workflow
			_, _, allShards, err := vsm.resolver.GetKeyspaceShards(ctx, sgtid.Keyspace, tabletType)
			if err != nil {
				return nil, nil, err
			}
			for _, shard := range allShards {
				newvgtid.ShardGtids = append(newvgtid.ShardGtids, &binlogdatapb.ShardGtid{
					Keyspace: sgtid.Keyspace,
					Shard:    shard.Name,
				})
			}
		} else {
			newvgtid.ShardGtids = append(newvgtid.ShardGtids, sgtid)
		}
	}
	for _, sgtid := range newvgtid.ShardGtids {
		if sgtid.Gtid == "" {
			sgtid.Gtid = "current"
		}
	}
	return filter, newvgtid, nil
}

func (vs *vstream) stream(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// mu protects sending on ch, err and positions.
	// mu is needed for sending because transactions can come
	// in separate chunks. If so, we have to send all the
	// chunks together.
	ch := make(chan []*binlogdatapb.VEvent)

	var wg sync.WaitGroup
	for _, sgtid := range vs.vgtid.ShardGtids {
		wg.Add(1)
		go func(sgtid *binlogdatapb.ShardGtid) {
			defer wg.Done()
			err := vs.vstreamOneShard(ctx, sgtid.Keyspace, sgtid.Shard, vs.tabletType, sgtid.Gtid, vs.filter, func(eventss [][]*binlogdatapb.VEvent) error {
				vs.mu.Lock()
				defer vs.mu.Unlock()

				// Send all chunks while holding the lock.
				for _, evs := range eventss {
					// Replace GTID and table names.
					for _, ev := range evs {
						switch ev.Type {
						case binlogdatapb.VEventType_GTID:
							// Update the VGtid and send that instead.
							sgtid.Gtid = ev.Gtid
							ev.Type = binlogdatapb.VEventType_VGTID
							ev.Gtid = ""
							ev.Vgtid = proto.Clone(vs.vgtid).(*binlogdatapb.VGtid)
						case binlogdatapb.VEventType_FIELD:
							ev.FieldEvent.TableName = sgtid.Keyspace + "." + ev.FieldEvent.TableName
						case binlogdatapb.VEventType_ROW:
							ev.RowEvent.TableName = sgtid.Keyspace + "." + ev.RowEvent.TableName
						}
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case ch <- evs:
					}
				}
				return nil
			})

			// Set the error on exit. First one wins.
			vs.mu.Lock()
			defer vs.mu.Unlock()
			if vs.err == nil {
				vs.err = err
				cancel()
			}
		}(sgtid)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for ev := range ch {
		if err := vs.send(ev); err != nil {
			return err
		}
	}

	return vs.err
}

// vstreamOneShard streams from one shard. If transactions come in separate chunks, they are grouped and sent.
func (vs *vstream) vstreamOneShard(ctx context.Context, keyspace, shard string, tabletType topodatapb.TabletType, startPos string, filter *binlogdatapb.Filter, send func(eventss [][]*binlogdatapb.VEvent) error) error {
	errCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var eventss [][]*binlogdatapb.VEvent
		rss, err := vs.resolver.ResolveDestination(ctx, keyspace, tabletType, key.DestinationShard(shard))
		if err != nil {
			return err
		}
		if len(rss) != 1 {
			// Unreachable.
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected number or shards: %v", rss)
		}
		err = rss[0].QueryService.VStream(ctx, rss[0].Target, startPos, filter, func(events []*binlogdatapb.VEvent) error {
			// Remove all heartbeat events for now.
			// Otherwise they can accumulate indefinitely if there are no real events.
			// TODO(sougou): figure out a model for this.
			for i := 0; i < len(events); i++ {
				if events[i].Type == binlogdatapb.VEventType_HEARTBEAT {
					events = append(events[:i], events[i+1:]...)
				}
			}
			if len(events) == 0 {
				return nil
			}
			// We received a valid event. Reset error count.
			errCount = 0

			eventss = append(eventss, events)
			lastEvent := events[len(events)-1]
			switch lastEvent.Type {
			case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL:
				if err := send(eventss); err != nil {
					return err
				}
				eventss = nil
			}
			return nil
		})
		if err == nil {
			// Unreachable.
			err = vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstream ended unexpectedly")
		}
		// Don't use isRetryableError here, because we're not using the discovery gateway, which retries on UNAVAILABLE.
		if vterrors.Code(err) != vtrpcpb.Code_FAILED_PRECONDITION && vterrors.Code(err) != vtrpcpb.Code_UNAVAILABLE {
			log.Errorf("vstream for %s/%s error: %v", keyspace, shard, err)
			return err
		}
		errCount++
		if errCount >= 3 {
			log.Errorf("vstream for %s/%s had three consecutive failures: %v", keyspace, shard, err)
			return err
		}
	}
}
