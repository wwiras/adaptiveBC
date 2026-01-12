apiVersion: v1
kind: Service
metadata:
  name: bcgossip-svc
  labels:
    run: bcgossip
spec:
  ports:
  - port: 5050
    protocol: TCP
  selector:
    run: bcgossip