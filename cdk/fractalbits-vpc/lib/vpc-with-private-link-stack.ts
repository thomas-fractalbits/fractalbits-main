import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as elbv2_targets from 'aws-cdk-lib/aws-elasticloadbalancingv2-targets';
import {createVpcEndpoints, createEc2Role} from './ec2-utils';
import {Instance} from "aws-cdk-lib/aws-ec2";
import {AccountRootPrincipal} from "aws-cdk-lib/aws-iam";

interface VpcWithPrivateLinkStackProps extends cdk.StackProps {
}

export class VpcWithPrivateLinkStack extends cdk.Stack {
  public readonly vpc: ec2.Vpc;

  constructor(scope: Construct, id: string, props: VpcWithPrivateLinkStackProps) {
    super(scope, id, props);

    this.vpc = new ec2.Vpc(this, 'VpcWithPrivateLink', {
      vpcName: 'vpc-with-private-link',
      ipAddresses: ec2.IpAddresses.cidr('10.0.0.0/16'),
      maxAzs: 2,
      natGateways: 0,
      subnetConfiguration: [
        {name: 'PrivateSubnet', subnetType: ec2.SubnetType.PRIVATE_ISOLATED, cidrMask: 24},
      ],
      enableDnsHostnames: true,
      enableDnsSupport: true,
    });

    const ec2Role = createEc2Role(this);
    createVpcEndpoints(this.vpc);

    // Security Group for instances
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc: this.vpc,
      securityGroupName: 'VpcWithPrivateLinkInstanceSG',
      description: 'Allow all outbound',
      allowAllOutbound: true,
    });
    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(80), 'Allow HTTP access from anywhere');

    const machineImage = ec2.MachineImage.latestAmazonLinux2023();
    const instanceType = ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO);

    const az1 = this.availabilityZones[0];
    const az2 = this.availabilityZones[1];

    const provider = new Instance(this, 'Provider', {
      vpc: this.vpc,
      vpcSubnets: {
        subnets: [this.vpc.isolatedSubnets[0]],
        availabilityZones: [az1],
      },
      role: ec2Role,
      securityGroup: sg,
      instanceType,
      machineImage,
    });
    provider.addUserData(
      'yum install -y httpd',
      'systemctl start httpd',
      'systemctl enable httpd',
      'echo "Hello from provider" > /var/www/html/index.html',
    );

    const nlb = new elbv2.NetworkLoadBalancer(this, 'VpcWithPrivateLinkNlb', {
      vpc: this.vpc,
      internetFacing: false,
      crossZoneEnabled: true,
      vpcSubnets: {
        subnets: this.vpc.isolatedSubnets,
      }
    });
    const listener = nlb.addListener('listener', {port: 80});
    listener.addTargets('target', {
      port: 80,
      targets: [new elbv2_targets.InstanceTarget(provider)],
    });

    const endpointService = new ec2.VpcEndpointService(this, 'EndpointService', {
      vpcEndpointServiceLoadBalancers: [nlb],
      allowedPrincipals: [new AccountRootPrincipal()],
      acceptanceRequired: false,
    });

    const endpoint = new ec2.InterfaceVpcEndpoint(this, 'Endpoint', {
      vpc: this.vpc,
      service: {
        name: endpointService.vpcEndpointServiceName,
        port: 80,
      },
      privateDnsEnabled: false,
      subnets: {
        subnets: this.vpc.isolatedSubnets,
      },
    });

    const consumer = new Instance(this, 'Consumer', {
      vpc: this.vpc,
      vpcSubnets: {
        subnets: [this.vpc.isolatedSubnets[1]],
        availabilityZones: [az2],
      },
      role: ec2Role,
      securityGroup: sg,
      instanceType,
      machineImage,
    });
    const endpointDnsEntry = cdk.Fn.select(0, endpoint.vpcEndpointDnsEntries);
    const endpointDns = cdk.Fn.select(1, cdk.Fn.split(':', endpointDnsEntry));
    consumer.addUserData(
      `echo "To send traffic, run the following command:"`,
      `echo "curl http://${endpointDns}"`,
    );

    new cdk.CfnOutput(this, 'ProviderInstanceId', {
      value: provider.instanceId,
      description: 'Provider instance ID',
    });
    new cdk.CfnOutput(this, 'ConsumerInstanceId', {
      value: consumer.instanceId,
      description: 'Consumer instance ID',
    });
    new cdk.CfnOutput(this, 'NlbName', {
      value: nlb.loadBalancerName,
      description: 'NLB name',
    });
    new cdk.CfnOutput(this, 'EndpointDns', {
      value: endpointDns,
      description: 'Endpoint DNS',
    });
    new cdk.CfnOutput(this, 'ProviderAZ', {
      value: az1,
      description: 'Provider availability zone',
    });
    new cdk.CfnOutput(this, 'ConsumerAZ', {
      value: az2,
      description: 'Consumer availability zone',
    });
  }
}
