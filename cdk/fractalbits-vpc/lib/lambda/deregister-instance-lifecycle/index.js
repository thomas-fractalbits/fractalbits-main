const { ServiceDiscoveryClient, ListInstancesCommand, DeregisterInstanceCommand } = require('@aws-sdk/client-servicediscovery');
const { AutoScalingClient, CompleteLifecycleActionCommand } = require("@aws-sdk/client-auto-scaling");
const servicediscovery = new ServiceDiscoveryClient({});
const autoscaling = new AutoScalingClient({});
exports.handler = async (event) => {
  console.log('Event: ', JSON.stringify(event, null, 2));
  const message = JSON.parse(event.Records[0].Sns.Message);
  const lifecycleHookName = message.LifecycleHookName;
  const autoScalingGroupName = message.AutoScalingGroupName;
  const lifecycleActionToken = message.LifecycleActionToken;
  const ec2InstanceId = message.EC2InstanceId;
  const serviceId = process.env.SERVICE_ID;
  if (!serviceId) {
    console.error('serviceId not found in the event message.');
    await completeLifecycleAction(lifecycleHookName, autoScalingGroupName, lifecycleActionToken, 'ABANDON', ec2InstanceId);
    return;
  }
  try {
    let cloudMapInstanceId;
    let nextToken;
    do {
      const command = new ListInstancesCommand({ ServiceId: serviceId, NextToken: nextToken });
      const data = await servicediscovery.send(command);
      const instance = data.Instances.find(
        (inst) => inst.Id === ec2InstanceId
      );
      if (instance) {
        cloudMapInstanceId = instance.Id;
        break;
      }
      nextToken = data.NextToken;
    } while (nextToken);
    if (cloudMapInstanceId) {
      console.log(`Deregistering instance ${cloudMapInstanceId} (EC2 instance ${ec2InstanceId}) from service ${serviceId}`);
      await servicediscovery.send(new DeregisterInstanceCommand({
        ServiceId: serviceId,
        InstanceId: cloudMapInstanceId,
      }));
      console.log('Deregistration successful.');
    } else {
      console.log(`Instance ${ec2InstanceId} not found in service ${serviceId}, nothing to deregister. It might have been deregistered already.`);
    }
    await completeLifecycleAction(lifecycleHookName, autoScalingGroupName, lifecycleActionToken, 'CONTINUE', ec2InstanceId);
  } catch (error) {
    console.error('Error during deregistration process:', error);
    await completeLifecycleAction(lifecycleHookName, autoScalingGroupName, lifecycleActionToken, 'ABANDON', ec2InstanceId);
  }
};
async function completeLifecycleAction(hookName, groupName, token, result, instanceId) {
  const params = {
    LifecycleHookName: hookName,
    AutoScalingGroupName: groupName,
    LifecycleActionToken: token,
    LifecycleActionResult: result,
    InstanceId: instanceId,
  };
  console.log(`Completing lifecycle action for instance ${instanceId} with result ${result}`);
  try {
    await autoscaling.send(new CompleteLifecycleActionCommand(params));
  } catch (err) {
    console.error(`Failed to complete lifecycle action: ${err}`);
  }
}
