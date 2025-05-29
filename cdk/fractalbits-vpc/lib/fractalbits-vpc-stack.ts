import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';

export class FractalbitsVpcStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create the VPC with 1 AZ and custom subnet configuration
    const vpc = new ec2.Vpc(this, 'FractalbitsVpc', {
      vpcName: 'fractalbits-vpc',
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: 'PublicSubnet',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
        {
          name: 'PrivateSubnet',
          subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
          cidrMask: 24,
        },
      ],
    });

    // Add S3 VPC endpoint
    vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [
        { subnetType: ec2.SubnetType.PUBLIC },
        { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      ],
    });

    // Add SSM VPC Endpoints (required for private subnet)
    vpc.addInterfaceEndpoint('SSMEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SSM,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    vpc.addInterfaceEndpoint('SSMMessagesEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    vpc.addInterfaceEndpoint('EC2MessagesEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
    });

    // IAM role for EC2 with SSM + S3
    const ec2Role = new iam.Role(this, 'InstanceRole', {
      roleName: 'FractalbitsInstanceRole',
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
    });
    ec2Role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')
    );
    ec2Role.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
    );

    // Security Group (no inbound rules; all outbound allowed)
    const sg = new ec2.SecurityGroup(this, 'InstanceSG', {
      vpc,
      securityGroupName: 'FractalbitsInstanceSG',
      description: 'Allow outbound only for SSM and S3 access',
      allowAllOutbound: true,
    });
    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(8888), 'Allow port 8888 from anywhere');
    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(9224), 'Allow port 9224 from anywhere');
    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(9225), 'Allow port 9225 from anywhere');
    sg.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(3000), 'Allow port 3000 from anywhere');

    // Amazon Linux 2023 AMI
    const ami = ec2.MachineImage.latestAmazonLinux2023();

    const apiServerInstance = new ec2.Instance(this, 'api_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      securityGroup: sg,
      role: ec2Role,
      privateIpAddress: "10.0.0.11",
    });
    apiServerInstance.userData.addCommands(
      'set -ex',
      'mkdir -p /opt/fractalbits/bin',
      'aws s3 cp s3://fractalbits-builds/fractalbits-bootstrap /opt/fractalbits/bin',
      'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
      '/opt/fractalbits/bin/fractalbits-bootstrap api-server',
    );

    const rootServerInstance = new ec2.Instance(this, 'root_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroup: sg,
      role: ec2Role,
      privateIpAddress: "10.0.1.254",
    });
    rootServerInstance.userData.addCommands(
      'set -ex',
      'mkdir -p /opt/fractalbits/bin',
      'aws s3 cp s3://fractalbits-builds/fractalbits-bootstrap /opt/fractalbits/bin',
      'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
      '/opt/fractalbits/bin/fractalbits-bootstrap root-server',
    );


    const bssServerInstance = new ec2.Instance(this, 'bss_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroup: sg,
      role: ec2Role,
      privateIpAddress: "10.0.1.10",
    });
    bssServerInstance.userData.addCommands(
      'set -ex',
      'mkdir -p /opt/fractalbits/bin',
      'aws s3 cp s3://fractalbits-builds/fractalbits-bootstrap /opt/fractalbits/bin',
      'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
      '/opt/fractalbits/bin/fractalbits-bootstrap bss-server',
    );

    const nssServerInstance = new ec2.Instance(this, 'nss_server', {
      vpc,
      instanceType: ec2.InstanceType.of(ec2.InstanceClass.T2, ec2.InstanceSize.MICRO),
      machineImage: ami,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      securityGroup: sg,
      role: ec2Role,
      privateIpAddress: "10.0.1.100",
    });
    nssServerInstance.userData.addCommands(
      'set -ex',
      'mkdir -p /opt/fractalbits/bin',
      'aws s3 cp s3://fractalbits-builds/fractalbits-bootstrap /opt/fractalbits/bin',
      'chmod +x /opt/fractalbits/bin/fractalbits-bootstrap',
      '/opt/fractalbits/bin/fractalbits-bootstrap nss-server',
    );

    // Outputs
    new cdk.CfnOutput(this, 'ApiServerId', {
      value: apiServerInstance.instanceId,
      description: 'EC2 instance API server ID',
    });

    new cdk.CfnOutput(this, 'RootServerId', {
      value: rootServerInstance.instanceId,
      description: 'EC2 instance root server ID',
    });

    new cdk.CfnOutput(this, 'BssServerId', {
      value: bssServerInstance.instanceId,
      description: 'EC2 instance bss server ID',
    });

    new cdk.CfnOutput(this, 'NssServerId', {
      value: nssServerInstance.instanceId,
      description: 'EC2 instance nss server ID',
    });

   new cdk.CfnOutput(this, 'ServicePublicIP', {
      value: apiServerInstance.instancePublicIp,
      description: 'Public IP of the API server',
    });
  }
}

