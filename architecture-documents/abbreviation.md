## Protocol & Standards
OCPP = Open Charge Point Protocol
OCPP-J = OCPP JSON variant (over WebSocket)
OCPP-S = OCPP SOAP variant (over HTTP)
OCA = Open Charge Alliance (maintains OCPP)
ISO 15118 = International standard for vehicle-to-grid communication

## Architecture & Systems
CSMS = Charging Station Management System (the backend/cloud)
CP = Charge Point (the physical charger)
EVSE = Electric Vehicle Supply Equipment (a charging position within a station)
CPMS = Charge Point Management System (synonym for CSMS)
V2G = Vehicle-to-Grid
V2X = Vehicle-to-Everything (bidirectional charging)
IaC = Infrastructure as Code

## OCPP Messages
CALL = Request message (TypeId 2)
CALLRESULT = Success response message (TypeId 3)
CALLERROR = Error response message (TypeId 4)

## Charger Telemetry
SoC = State of Charge (battery percentage)
Wh = Watt-hours (energy unit)
kWh = Kilowatt-hours

## Measurands
Energy.Active.Import.Register = Cumulative energy delivered to vehicle
Energy.Active.Export.Register = Cumulative energy returned to grid
Power.Active.Import = Current power draw (W)
Current.Import = Current flowing to vehicle (A)

## Transaction & Auth
idTag = Identifier tag (e.g. RFID card ID) used for authorization
RFID = Radio-Frequency Identification

## Security
TLS = Transport Layer Security
mTLS = Mutual TLS (client certificate authentication)

## Infrastructure & DevOps
API = Application Programming Interface
REST = Representational State Transfer
UUID = Universally Unique Identifier
CI/CD = Continuous Integration / Continuous Deployment
K8s = Kubernetes
EKS = Elastic Kubernetes Service (AWS)
GKE = Google Kubernetes Engine
AKS = Azure Kubernetes Service
DynamoDB = AWS managed NoSQL database
S3 = AWS Simple Storage Service
SNS = Simple Notification Service (AWS)
SQS = Simple Queue Service (AWS)

## Data & Messaging
JSON = JavaScript Object Notation
MQTT = Message Queuing Telemetry Transport
WS / WSS = WebSocket / WebSocket Secure