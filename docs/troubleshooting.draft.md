## Troubleshooting

### Common Issues

1. **Rule conflicts**: Multiple rules targeting the same taint key
   ```sh
   # Check validation webhook logs
   kubectl logs -n nrrcontroller-system deployment/nrrcontroller-controller-manager | grep webhook
   ```

2. **Missing node conditions**: Rules waiting for conditions that don't exist
   ```sh
   # Check node conditions
   kubectl describe node <node-name> | grep Conditions -A 20

   # Check rule evaluation status
   kubectl get nodereadinessrule <rule-name> -o yaml | grep nodeEvaluations -A 50
   ```

3. **RBAC issues**: Controller can't update nodes or rules
   ```sh
   # Check controller logs for permission errors
   kubectl logs -n nrrcontroller-system deployment/nrrcontroller-controller-manager

   # Verify RBAC
   kubectl describe clusterrole nrrcontroller-manager-role
   ```

### Bootstrap Completion Tracking

For bootstrap-only rules, completion is tracked via node annotations:

```sh
# Check if bootstrap completed for a node
kubectl get node <node-name> -o jsonpath='{.metadata.annotations}'

# Look for: readiness.k8s.io/bootstrap-completed-<ruleName>=true
```

### Verification

Check that the controller is running:

```sh
kubectl get pods -n nrrcontroller-system
kubectl logs -n nrrcontroller-system deployment/nrrcontroller-controller-manager
```

Verify CRDs are installed:

```sh
kubectl get crd nodereadinessrules.readiness.node.x-k8s.io
```


### Debugging

Enable verbose logging:

```sh
# Edit controller deployment to add debug flags
kubectl patch deployment -n nrrcontroller-system nrrcontroller-controller-manager \
  -p '{"spec":{"template":{"spec":{"containers":[{"name":"manager","args":["--zap-log-level=debug"]}]}}}}'
```

### Rule stuck in Terminating

A `NodeReadinessRule` stays in `Terminating` until every taint it applied has
been removed from the affected Nodes. This barrier is fail-closed: the
finalizer (`readiness.node.x-k8s.io/cleanup-taints`) is never removed
automatically while any Node still records the rule's taint in its ownership
ledger (annotation `readiness.k8s.io/owned-taints`).

If a rule sits in `Terminating`, the controller emits a Warning event once the
wait passes a threshold:

```sh
kubectl describe nodereadinessrule <name>   # look for reason TaintDrainPending
```

The usual cause is a Node whose taint removal keeps failing. Find it:

```sh
# Nodes still holding the rule's taint key in their ledger
kubectl get nodes -o json \
  | jq -r --arg k '<taint-key>' '.items[]
      | select((.metadata.annotations["readiness.k8s.io/owned-taints"] // "")
      | contains($k)) | .metadata.name'

# Look for TaintRemoveFailed events on those Nodes
kubectl describe node <node>
```

Fix the underlying cause (API errors, a wedged Node object) and the drain
completes on its own. Only as a last resort, and once you have confirmed the
taints are actually gone, remove the finalizer by hand to force deletion:

```sh
kubectl patch nodereadinessrule <name> --type=json \
  -p '[{"op":"remove","path":"/metadata/finalizers/0"}]'
```
