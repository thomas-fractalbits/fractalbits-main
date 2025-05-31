#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { FractalbitsVpcStack } from '../lib/fractalbits-vpc-stack';

const app = new cdk.App();
new FractalbitsVpcStack(app, 'FractalbitsVpcStack', {
  env: {},
});

