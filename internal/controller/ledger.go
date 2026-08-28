/*
Copyright The Kubernetes Authors.

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

package controller

import (
	"encoding/json"
	"sort"

	corev1 "k8s.io/api/core/v1"
)

// ownedTaintsAnnotation is this controller's provenance ledger on a Node: the
// set of taint IDENTITIES (key+effect) it applied. It is written in the SAME
// patch as the taint add/remove so ownership and the taint can never diverge.
//
// Fail-closed GC relies on this: a taint whose identity is NOT in the ledger is
// provably not ours (foreign) and must never be removed. A corrupt or absent
// ledger reads as the empty set, so GC claims nothing — the safe direction.
const ownedTaintsAnnotation = "readiness.k8s.io/owned-taints"

// taintID is a taint's ledger identity: key AND effect. A Node may carry two
// taints that share a key but differ in effect (two rules), and each must be
// tracked independently — tracking by key alone lets removing one drop the
// other's ownership record, after which fail-closed GC can never sweep it.
func taintID(t corev1.Taint) string {
	return t.Key + "\x00" + string(t.Effect)
}

// ownedTaintIDs returns the set of taint identities this controller has recorded
// as owned on the node. Unparseable content is treated as empty (fail-closed).
func ownedTaintIDs(node *corev1.Node) map[string]struct{} {
	out := map[string]struct{}{}
	raw := node.Annotations[ownedTaintsAnnotation]
	if raw == "" {
		return out
	}
	var keys []string
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return out
	}
	for _, k := range keys {
		out[k] = struct{}{}
	}
	return out
}

// setOwnedTaintIDs writes the ledger annotation from an identity set, deleting
// the annotation entirely when the set is empty. Entries are sorted so an
// unchanged set always serialises identically and never produces a spurious
// patch.
func setOwnedTaintIDs(node *corev1.Node, keys map[string]struct{}) {
	if len(keys) == 0 {
		delete(node.Annotations, ownedTaintsAnnotation)
		return
	}
	list := make([]string, 0, len(keys))
	for k := range keys {
		list = append(list, k)
	}
	sort.Strings(list)
	b, err := json.Marshal(list)
	if err != nil {
		return // list is []string; marshal cannot fail, but stay defensive.
	}
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[ownedTaintsAnnotation] = string(b)
}

// ownedTaintsEqual reports whether two nodes carry the same ownership ledger.
func ownedTaintsEqual(a, b *corev1.Node) bool {
	ka, kb := ownedTaintIDs(a), ownedTaintIDs(b)
	if len(ka) != len(kb) {
		return false
	}
	for k := range ka {
		if _, ok := kb[k]; !ok {
			return false
		}
	}
	return true
}

// addOwnedTaint records taint t as owned on the node (in-memory; caller patches).
func addOwnedTaint(node *corev1.Node, t corev1.Taint) {
	ids := ownedTaintIDs(node)
	ids[taintID(t)] = struct{}{}
	setOwnedTaintIDs(node, ids)
}

// removeOwnedTaint drops taint t from the node's ledger (in-memory; caller patches).
func removeOwnedTaint(node *corev1.Node, t corev1.Taint) {
	ids := ownedTaintIDs(node)
	delete(ids, taintID(t))
	setOwnedTaintIDs(node, ids)
}
