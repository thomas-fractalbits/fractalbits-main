#!/bin/bash

NAMESPACE_NAME="fractalbits.local"

# Get all Namespace IDs that match the name
NAMESPACE_IDS=$(aws servicediscovery list-namespaces --query "Namespaces[?Name=='${NAMESPACE_NAME}'].Id" --output text)

if [ -z "$NAMESPACE_IDS" ]; then
  echo "No namespaces found with name '$NAMESPACE_NAME'."
  exit 0
fi

for NAMESPACE_ID in $NAMESPACE_IDS;
do
  echo "Processing Namespace ID: $NAMESPACE_ID for Namespace: $NAMESPACE_NAME"

  # Get all Service IDs within this namespace
  SERVICE_IDS=$(aws servicediscovery list-services --filters "Name=NAMESPACE_ID,Values=$NAMESPACE_ID" --query "Services[*].Id" --output text)

  if [ -z "$SERVICE_IDS" ]; then
    echo "  No services found in Namespace ID '$NAMESPACE_ID'."
    continue
  fi

  for SERVICE_ID in $SERVICE_IDS;
  do
    SERVICE_NAME_CURRENT=$(aws servicediscovery list-services --filters "Name=NAMESPACE_ID,Values=$NAMESPACE_ID" --query "Services[?Id=='${SERVICE_ID}'].Name" --output text)
    echo "  Processing Service ID: $SERVICE_ID (Name: $SERVICE_NAME_CURRENT)"

    # List and Deregister Instances
    INSTANCE_IDS=$(aws servicediscovery list-instances --service-id "$SERVICE_ID" --query "Instances[*].Id" --output text)

    if [ -z "$INSTANCE_IDS" ]; then
      echo "    No instances found for service '$SERVICE_NAME_CURRENT'."
    else
      echo "    Deregistering instances for service '$SERVICE_NAME_CURRENT':"
      for INSTANCE_ID in $INSTANCE_IDS;
      do
        echo "      Deregistering instance: $INSTANCE_ID"
        aws servicediscovery deregister-instance --service-id "$SERVICE_ID" --instance-id "$INSTANCE_ID"
        if [ $? -ne 0 ]; then
          echo "      Failed to deregister instance $INSTANCE_ID. Continuing with others."
        fi
      done
      echo "    Deregistration complete for service '$SERVICE_NAME_CURRENT'."
    fi
  done
done

echo "CloudMap cleanup complete."
