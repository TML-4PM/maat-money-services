/**
 * email_archive_4pm_cdk_stack.ts
 * CDK stack — deploys both Lambdas + EventBridge auto-schedules
 * Account: 140548542136 | Region: ap-southeast-2
 *
 * Usage:
 *   cdk deploy EmailArchive4pmStack
 */

import * as cdk   from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as iam    from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as path from 'path';

// ── config ────────────────────────────────────────────────────
const BRIDGE_URL  = 'https://m5oqj21chd.execute-api.ap-southeast-2.amazonaws.com/lambda/invoke';
const REGION      = 'ap-southeast-2';
const ACCOUNT     = '140548542136';

export class EmailArchive4pmStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, {
      ...props,
      env: { account: ACCOUNT, region: REGION },
    });

    // ── secrets (already in Secrets Manager from cap_secrets)
    const supabaseSecret = secretsmanager.Secret.fromSecretNameV2(
      this, 'SupabaseSecret', 'email-archive/supabase'
    );
    const gmailSaSecret = secretsmanager.Secret.fromSecretNameV2(
      this, 'GmailSASecret', 'email-archive/gmail-service-account'
    );
    const anthropicSecret = secretsmanager.Secret.fromSecretNameV2(
      this, 'AnthropicSecret', 'email-archive/anthropic-key'
    );

    // ── shared env
    const sharedEnv = {
      NODE_OPTIONS:    '--enable-source-maps',
      SUPABASE_URL:    'https://lzfgigiyqpuuxslsygjt.supabase.co',
      // secrets injected at runtime via secretsmanager env injection below
    };

    // ── /run Lambda
    const runFn = new lambda.Function(this, 'EmailArchiveRun', {
      functionName:  'email-archive-4pm-run',
      runtime:       lambda.Runtime.NODEJS_20_X,
      handler:       'email_archive_4pm_lambda_run.handler',
      code:          lambda.Code.fromAsset(path.join(__dirname, 'dist')),
      timeout:       cdk.Duration.minutes(15),
      memorySize:    1024,
      environment:   {
        ...sharedEnv,
        SUPABASE_SERVICE_KEY: supabaseSecret.secretValueFromJson('service_key').unsafeUnwrap(),
        GMAIL_SERVICE_ACCOUNT_JSON: gmailSaSecret.secretValue.unsafeUnwrap(),
      },
    });
    supabaseSecret.grantRead(runFn);
    gmailSaSecret.grantRead(runFn);

    // ── /enrich Lambda
    const enrichFn = new lambda.Function(this, 'EmailArchiveEnrich', {
      functionName:  'email-archive-4pm-enrich',
      runtime:       lambda.Runtime.NODEJS_20_X,
      handler:       'email_archive_4pm_lambda_enrich.handler',
      code:          lambda.Code.fromAsset(path.join(__dirname, 'dist')),
      timeout:       cdk.Duration.minutes(15),
      memorySize:    1024,
      environment:   {
        ...sharedEnv,
        SUPABASE_SERVICE_KEY:       supabaseSecret.secretValueFromJson('service_key').unsafeUnwrap(),
        ANTHROPIC_API_KEY:          anthropicSecret.secretValue.unsafeUnwrap(),
      },
    });
    supabaseSecret.grantRead(enrichFn);
    anthropicSecret.grantRead(enrichFn);

    // ── API Gateway routes (wired into existing Bridge API)
    // If you want standalone routes, uncomment below.
    // For Bridge-style, just add these Lambdas to the existing Bridge allowlist.
    //
    // const api = new apigateway.LambdaRestApi(this, 'EmailArchiveApi', {
    //   handler: runFn,
    //   proxy:   false,
    // });
    // const archive = api.root.addResource('bridge').addResource('email-archive').addResource('4pm');
    // archive.addResource('run').addMethod('POST', new apigateway.LambdaIntegration(runFn));
    // archive.addResource('enrich').addMethod('POST', new apigateway.LambdaIntegration(enrichFn));
    // archive.addResource('enrich').addResource('summary').addMethod('GET', new apigateway.LambdaIntegration(enrichFn));

    // ── EventBridge: daily incremental sync (1am AEST = 15:00 UTC)
    new events.Rule(this, 'DailyIncrementalSync', {
      ruleName:    'email-archive-4pm-daily-sync',
      description: 'Daily incremental Gmail sync for Troy.Latter@4pm.net.au',
      schedule:    events.Schedule.cron({ hour: '15', minute: '0' }),
      targets:     [
        new targets.LambdaFunction(runFn, {
          event: events.RuleTargetInput.fromObject({
            httpMethod: 'POST',
            body: JSON.stringify({ mode: 'incremental' }),
          }),
          retryAttempts: 2,
        }),
      ],
    });

    // ── EventBridge: weekly enrichment (Monday 2am AEST = 16:00 UTC Sun)
    new events.Rule(this, 'WeeklyEnrichment', {
      ruleName:    'email-archive-4pm-weekly-enrich',
      description: 'Weekly sentiment + gap + pattern enrichment',
      schedule:    events.Schedule.cron({ weekDay: 'MON', hour: '16', minute: '0' }),
      targets:     [
        new targets.LambdaFunction(enrichFn, {
          event: events.RuleTargetInput.fromObject({
            httpMethod: 'POST',
            body: JSON.stringify({ passes: ['sentiment', 'missing', 'patterns'] }),
          }),
          retryAttempts: 1,
        }),
      ],
    });

    // ── Grant EventBridge permission to invoke
    runFn.addPermission('AllowEventBridgeRun', {
      principal: new iam.ServicePrincipal('events.amazonaws.com'),
    });
    enrichFn.addPermission('AllowEventBridgeEnrich', {
      principal: new iam.ServicePrincipal('events.amazonaws.com'),
    });

    // ── Outputs
    new cdk.CfnOutput(this, 'RunFunctionArn',    { value: runFn.functionArn });
    new cdk.CfnOutput(this, 'EnrichFunctionArn', { value: enrichFn.functionArn });
  }
}

const app = new cdk.App();
new EmailArchive4pmStack(app, 'EmailArchive4pmStack');
