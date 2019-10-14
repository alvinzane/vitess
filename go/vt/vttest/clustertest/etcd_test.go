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

package clustertest

import (
	"testing"
)

func TestEtcdServer(t *testing.T) {
	testURL(t, "http://localhost:2379/v2/keys", "generic etcd url")
	testURL(t, "http://localhost:2379/v2/keys/vitess/global", "vitess global key")
	testURL(t, "http://localhost:2379/v2/keys/vitess/zone1", "vitess zone1 key")
}
