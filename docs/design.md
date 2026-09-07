# Design Goals

1. Traffic Director intends to serve as reverse proxy for backend MQTT clusters, supporting MQTT v3.1.1 and v5.0;
2. Traffic Director intends to achieve zero-downtime its upgrades and backend MQTT broker restarts, without disrupting existing connections;
3. Traffic Director, after integrating Istio via xDS protocol, is to direct traffic among multiple MQTT clusters to survive disasters;