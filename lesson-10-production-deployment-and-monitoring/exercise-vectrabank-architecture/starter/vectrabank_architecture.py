"""
vectrabank_architecture.py - EXERCISE STARTER (Student-Led)
==============================================================
Module 10 Exercise: Plan a Production Deployment Architecture for VectraBank

Create a deployment architecture plan for VectraBank's financial services
multi-agent system. Define runtime configuration, monitoring strategy,
cost estimates, and operational runbooks.

Same planning pattern as the demo (deployment_walkthrough.py),
with additions:
  1. FINANCIAL DOMAIN — VectraBank-specific agents and KBs
  2. OPERATIONAL RUNBOOK (NEW) — deploy, rollback, kill switch, latency
  3. COMPLIANCE REQUIREMENTS — VPC network mode, stricter thresholds
  4. COST OPTIMIZATION — model selection recommendations

Instructions:
  - Follow the demo pattern (deployment_walkthrough.py)
  - Look for TODO 1-8 below
  - Define configs as Python dicts; the provided helper handles deployment
  - Focus on WHAT to configure and WHY
"""

import json
import os
from pathlib import Path
import agentcore_cli
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


# ═══════════════════════════════════════════════════════
#  VECTRABANK RUNTIME CONFIGURATION
# ═══════════════════════════════════════════════════════

# TODO 1: Define the AgentCore Runtime configuration
# Hint: Same structure as demo, but:
#   - Use networkMode "PUBLIC" and serverProtocol "HTTP" in this lab
#   - Document private subnets and security groups for a production VPC
#   - Role and guardrail IDs are discovered by the provided helper
#   - Environment variables: 3 KB IDs, state table, audit table, region, log level
VECTRABANK_RUNTIME_CONFIG = {
    "agentRuntimeName": "vectrabank-financial-services",
    "description": "Multi-agent system for VectraBank financial services",
    "roleArn": os.environ.get("AGENTCORE_ROLE_ARN", "<from-cf-exports>"),
    # Financial services → internal-only, no public internet exposure
    "networkConfiguration": {
        "networkMode": "VPC",
        "vpcConfiguration": {
            "vpcId": "vpc-vectrabank",
            "subnetIds": ["subnet-private-1a", "subnet-private-1b"],
            "securityGroupIds": ["sg-vectrabank-agents"],
        },
    },
    "protocolConfiguration": {"serverProtocol": "MCP"},
    "guardrailConfiguration": {
        "guardrailIdentifier": "gr-vectrabank-compliance",
        "guardrailVersion": "1",
    },
    "environmentVariables": {
        "MARKET_DATA_KB_ID": "KB-MARKET-001",
        "COMPLIANCE_KB_ID": "KB-COMPLIANCE-002",
        "RISK_ASSESSMENT_KB_ID": "KB-RISK-003",
        "STATE_TABLE_NAME": "vectrabank-state",
        "AUDIT_TABLE_NAME": "vectrabank-audit",
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "LOG_LEVEL": "INFO",
        "ENVIRONMENT": "production",
    },
}


# ═══════════════════════════════════════════════════════
#  AGENT DEFINITIONS
# ═══════════════════════════════════════════════════════

# TODO 2: Define 4 agents for VectraBank
# Hint: QueryRouter (Nova Lite), MarketDataRetriever (Nova Lite),
#       ComplianceRetriever (Nova Lite), FinancialAdvisor (Claude Sonnet)
#   Each needs: name, model, temperature, role, tools, estimated_tokens, requests_per_day
VECTRABANK_AGENTS = [
    {
        "name": "QueryRouter",
        "model": "amazon.nova-lite-v1:0",
        "temperature": 0.0,
        "role": "Route each client query to the correct specialist agent",
        "tools": ["classify_query", "check_client_tier"],
        "estimated_tokens_per_request": 500,
        "requests_per_day": 10000,   # every request hits the router
    },
    {
        "name": "MarketDataRetriever",
        "model": "amazon.nova-lite-v1:0",
        "temperature": 0.0,
        "role": "Retrieve market data passages from the Market Data KB",
        "tools": ["retrieve_market_data"],
        "estimated_tokens_per_request": 800,
        "requests_per_day": 6000,
    },
    {
        "name": "ComplianceRetriever",
        "model": "amazon.nova-lite-v1:0",
        "temperature": 0.0,
        "role": "Retrieve regulatory/compliance passages from the Compliance KB",
        "tools": ["retrieve_compliance"],
        "estimated_tokens_per_request": 800,
        "requests_per_day": 6000,
    },
    {
        "name": "FinancialAdvisor",
        "model": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        "temperature": 0.1,
        "role": "Synthesize a grounded, compliant financial recommendation with citations",
        "tools": ["synthesize_advice"],
        "estimated_tokens_per_request": 2500,
        "requests_per_day": 4000,   # ~40% of queries need full synthesis
    },
]


# ═══════════════════════════════════════════════════════
#  MONITORING STRATEGY
# ═══════════════════════════════════════════════════════

# TODO 3: Define the monitoring dashboard (6 widgets)
# Hint: Total Queries, Latency P50/P99, Error Rate (2% threshold),
#       Guardrail Blocks, RAG Quality (NEW), Kill Switch Status (NEW)

# TODO 4: Define 3 alarms with thresholds
# Hint: HighErrorRate (2%), HighLatencyP99 (8s), GuardrailViolationSpike (50 blocks/5min)

# TODO 5: Define X-Ray tracing config
# Hint: 10% sampling for financial audit, annotations for query_type, agent_name, etc.
VECTRABANK_MONITORING = {
    "dashboard_name": "vectrabank-financial-services",
    "widgets": [
        {"title": "Total Queries", "type": "line", "metric": "AgentCore/Invocations",
         "period": 60, "stat": "Sum"},
        {"title": "Latency P50/P99", "type": "line",
         "metrics": [{"name": "AgentCore/Latency", "stat": "p50"},
                     {"name": "AgentCore/Latency", "stat": "p99"}], "period": 60},
        {"title": "Error Rate", "type": "number", "metric": "AgentCore/Errors",
         "period": 300, "stat": "Average", "threshold": 0.02},   # 2% — stricter for finance
        {"title": "Guardrail Blocks by Type", "type": "stacked_bar",
         "metrics": [{"name": "Guardrail/ContentBlocks", "label": "Content"},
                     {"name": "Guardrail/PIIBlocks", "label": "PII"},
                     {"name": "Guardrail/TopicBlocks", "label": "Topic"}], "period": 300},
        {"title": "RAG Retrieval Quality (avg score)", "type": "line",   # NEW
         "metric": "RAG/AvgRelevanceScore", "period": 300, "stat": "Average"},
        {"title": "Kill Switch Status", "type": "number",   # NEW
         "metric": "Governance/KillSwitchTriggered", "period": 60, "stat": "Maximum"},
    ],
    "alarms": [
        {"name": "HighErrorRate", "metric": "AgentCore/Errors", "threshold": 0.02,
         "period": 300, "action": "SNS → kill-switch-topic → Lambda disables runtime"},
        {"name": "HighLatencyP99", "metric": "AgentCore/Latency", "stat": "p99",
         "threshold": 8.0, "period": 300, "action": "SNS → ops-team-pager"},
        {"name": "GuardrailViolationSpike", "metric": "Guardrail/TotalBlocks",
         "threshold": 50, "period": 300,   # 50 blocks / 5 min → possible attack
         "action": "SNS → security-team + compliance"},
    ],
    "xray_tracing": {
        "enabled": True,
        "sampling_rate": 0.10,   # 10% — higher for financial audit trail
        "annotations": ["query_type", "agent_name", "model_id", "client_tier", "compliance_flag"],
    },
}


# ═══════════════════════════════════════════════════════
#  COST ESTIMATION (provided — same as demo)
# ═══════════════════════════════════════════════════════

MODEL_PRICING = {
    "amazon.nova-lite-v1:0": {"input": 0.00006, "output": 0.00024},
    "amazon.nova-pro-v1:0": {"input": 0.0008, "output": 0.0032},
    "us.anthropic.claude-sonnet-4-5-20250929-v1:0": {"input": 0.003, "output": 0.015},
}


def estimate_monthly_costs(agents: list, days: int = 30) -> dict:
    """Estimate monthly costs for VectraBank."""
    costs = {}
    total = 0

    for agent in agents:
        model = agent["model"]
        pricing = MODEL_PRICING.get(model, {"input": 0.001, "output": 0.005})
        tokens = agent["estimated_tokens_per_request"]
        daily_requests = agent["requests_per_day"]

        input_tokens = tokens * 0.6
        output_tokens = tokens * 0.4
        daily_cost = ((input_tokens / 1000) * pricing["input"] +
                      (output_tokens / 1000) * pricing["output"]) * daily_requests
        monthly_cost = daily_cost * days

        costs[agent["name"]] = {
            "model": model, "daily_requests": daily_requests,
            "monthly_cost": round(monthly_cost, 2),
        }
        total += monthly_cost

    # TODO 6: Add infrastructure costs
    # Hint: DynamoDB (2 tables), Knowledge Bases (3 KBs), CloudWatch + X-Ray, VPC
    # Add each as costs["name"] = {"monthly_cost": estimated_cost}
    infrastructure_costs = {
        "dynamodb": {
            "table_state": 25.0,   # on-demand, ~10K reads/writes per day
            "table_audit": 15.0,   # compliance: write-heavy, 7-day+ retention
        },
        "knowledge_bases": {
            "market_data_kb": 30.0,     # S3 Vectors storage + embeddings + queries
            "compliance_kb": 30.0,
            "risk_assessment_kb": 30.0,
        },
        "cloudwatch": {
            "logs": 20.0,       # ingestion + storage at INFO level, prod volume
            "metrics": 10.0,    # custom metrics (per-agent latency, guardrail counts)
            "dashboard": 3.0,   # $3/dashboard/month
        },
        "xray": {
            "sampling_and_analysis": 15.0,  # 10% sampling (higher than demo's 5%)
        },
        "vpc": {
            "nat_gateway": 32.0,    # ~$0.045/hr + data processing
            "vpc_endpoints": 22.0,  # interface endpoints for Bedrock + S3
        },
    }

    # TODO 6l: Calculate total infrastructure costs and add to costs dict
    # Example: costs["DynamoDB"] = {"monthly_cost": sum of dynamodb costs}
    #         costs["Knowledge Bases"] = {"monthly_cost": sum of KB costs}
    #         etc.
    # Then add infrastructure total to `total` before computing TOTAL below:
    infra_total = sum(
        cost for category in infrastructure_costs.values()
        for cost in category.values()
    )
    total += infra_total

    # TODO 6l: per-category display rows (already counted in infra_total above — display only)
    costs["DynamoDB"] = {"monthly_cost": round(sum(infrastructure_costs["dynamodb"].values()), 2)}
    costs["Knowledge Bases"] = {"monthly_cost": round(sum(infrastructure_costs["knowledge_bases"].values()), 2)}
    costs["CloudWatch"] = {"monthly_cost": round(sum(infrastructure_costs["cloudwatch"].values()), 2)}
    costs["X-Ray"] = {"monthly_cost": round(sum(infrastructure_costs["xray"].values()), 2)}
    costs["VPC"] = {"monthly_cost": round(sum(infrastructure_costs["vpc"].values()), 2)}

    costs["TOTAL"] = {"monthly_cost": round(total, 2)}
    return costs


# ═══════════════════════════════════════════════════════
#  OPERATIONAL RUNBOOK (NEW — not in demo)
# ═══════════════════════════════════════════════════════

# TODO 7: Define 4 operational runbook procedures
# Hint: Each is a dict with "name", "steps" (list of strings), and procedure-specific fields
#   1. Deploy a New Version — test, update guardrail, deploy runtime, smoke test, monitor
#   2. Rollback — identify previous version, update runtime, verify, post-mortem
#   3. Kill Switch Triggered — acknowledge, check audit log, investigate, fix, re-enable
#   4. Latency Investigation — X-Ray map, per-agent latency, identify bottleneck, scale
OPERATIONAL_RUNBOOK = {
    "deploy": {
        "name": "Production Deployment",
        "steps": [
            "1. Run the full agent test suite — all task2-task6 tests must pass",
            "2. Validate the guardrail config and confirm a non-DRAFT version is promoted",
            "3. Deploy the new AgentCore Runtime version (create/update agent runtime)",
            "4. Run smoke tests on 10 representative transactions against the new version",
            "5. Monitor error rate and P99 latency for 5 minutes before shifting full traffic",
        ],
        "rollback_trigger": "Error rate > 2% OR P99 latency > 8s during the 5-minute watch",
        "estimated_duration_minutes": 15,  # Estimate time to complete
    },

    "rollback": {
        "name": "Emergency Rollback",
        "steps": [
            "1. Identify the previous stable runtime version from the deploy history",
            "2. Revert the runtime to that previous version (update agent runtime)",
            "3. Verify agents respond correctly on the 10 smoke-test transactions",
            "4. Hold a post-incident review — root-cause what the new version broke",
        ],
        "estimated_duration_minutes": 10,
    },

    "kill_switch": {
        "name": "Kill Switch Activation",
        "steps": [
            "1. Acknowledge the alert in PagerDuty to stop escalation",
            "2. Check the X-Ray service map for error/latency patterns across agents",
            "3. Check CloudWatch audit logs for anomalies (spike source, repeated inputs)",
            "4. Disable the runtime if an attack or fraud pattern is confirmed",
            "5. Notify the compliance team (required for a financial-system shutdown)",
        ],
        "threshold": "Guardrail blocks > 50 in 5 minutes (GuardrailViolationSpike alarm)",
        "requires_approval": True,  # Financial system requires human approval
        "estimated_duration_minutes": 5,
    },

    "latency_investigation": {
        "name": "High Latency Investigation",
        "steps": [
            "1. Pull the X-Ray service map to find which node is slow",
            "2. Check per-agent latency in CloudWatch to isolate the agent",
            "3. Identify the bottleneck — orchestrator, a worker agent, or KB retrieval",
            "4. Check for DynamoDB throttling or elevated KB retrieval latency",
            "5. Scale the bottleneck (provisioned capacity / parallelism) and re-measure",
        ],
        "trigger_threshold": "P99 latency > 8 seconds",
        "estimated_duration_minutes": 20,
    },
}


# ═══════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════

def deploy_to_agentcore() -> str:
    """Provided infrastructure; student architecture TODOs remain above."""
    required = ("networkConfiguration", "protocolConfiguration", "environmentVariables")
    if any(key not in VECTRABANK_RUNTIME_CONFIG for key in required) or len(VECTRABANK_AGENTS) != 4:
        raise ValueError("Complete the runtime configuration and four agent definitions before deployment.")
    resources = agentcore_cli.load_resources("lesson-10-exercise")
    config = {**VECTRABANK_RUNTIME_CONFIG,
              "roleArn": resources["AgentCoreRoleArn"],
              "guardrailConfiguration": {
                  "guardrailIdentifier": resources["GuardrailId"],
                  "guardrailVersion": os.environ.get("GUARDRAIL_VERSION", "DRAFT")}}
    return agentcore_cli.deploy(config)


def main():
    print("=" * 70)
    print("  VectraBank Deployment Architecture — Module 10 Exercise")
    print("  Runtime Config + Monitoring + Cost + Operational Runbook")
    print("=" * 70)

    # ── Runtime Configuration ──
    print(f"\n{'━' * 70}")
    print("  1. AgentCore Runtime Configuration")
    print(f"{'━' * 70}")

    # TODO 8: Print runtime config, agents, monitoring strategy, and runbook
    rc = VECTRABANK_RUNTIME_CONFIG
    print(f"  Runtime:  {rc['agentRuntimeName']}")
    print(f"  Network:  {rc['networkConfiguration']['networkMode']} (internal-only)")
    print(f"  Guardrail: {rc['guardrailConfiguration']['guardrailIdentifier']} "
          f"v{rc['guardrailConfiguration']['guardrailVersion']}")
    print(f"  Env vars: {', '.join(rc['environmentVariables'].keys())}")

    print(f"\n{'━' * 70}")
    print("  2. Agent Definitions")
    print(f"{'━' * 70}")
    for a in VECTRABANK_AGENTS:
        print(f"  {a['name']:<22s} {a['model']:<48s} temp={a['temperature']}")
        print(f"    role: {a['role']}")

    print(f"\n{'━' * 70}")
    print("  3. Monitoring Strategy")
    print(f"{'━' * 70}")
    m = VECTRABANK_MONITORING
    print(f"  Dashboard '{m['dashboard_name']}' — {len(m['widgets'])} widgets:")
    for w in m["widgets"]:
        print(f"    • {w['title']}")
    print(f"  Alarms ({len(m['alarms'])}):")
    for al in m["alarms"]:
        print(f"    • {al['name']}: threshold={al['threshold']} → {al['action']}")
    print(f"  X-Ray: {int(m['xray_tracing']['sampling_rate']*100)}% sampling, "
          f"annotations={m['xray_tracing']['annotations']}")

    print(f"\n{'━' * 70}")
    print("  5. Operational Runbook")
    print(f"{'━' * 70}")
    for key, proc in OPERATIONAL_RUNBOOK.items():
        print(f"\n  {proc['name']} (~{proc['estimated_duration_minutes']} min):")
        for step in proc["steps"]:
            print(f"    {step}")

    if VECTRABANK_AGENTS:
        # ── Cost Estimation ──
        print(f"\n{'━' * 70}")
        print("  4. Monthly Cost Estimation (10,000 requests/day)")
        print(f"{'━' * 70}")
        costs = estimate_monthly_costs(VECTRABANK_AGENTS)
        print(f"\n  {'Component':<25s} {'Model':<40s} {'Monthly':>10s}")
        print(f"  {'─' * 75}")
        for name, data in costs.items():
            if name == "TOTAL":
                print(f"  {'─' * 75}")
            model = data.get("model", "—")
            cost = data["monthly_cost"]
            print(f"  {name:<25s} {model:<40s} ${cost:>9.2f}")

    print(f"\n  Key Takeaways:")
    print(f"  1. VPC NETWORK MODE — financial services agents stay internal")
    print(f"  2. MULTI-MODEL COST OPTIMIZATION — Lite for routing/retrieval, Sonnet for synthesis")
    print(f"  3. STRICTER THRESHOLDS — 2% error rate for financial compliance")
    print(f"  4. OPERATIONAL RUNBOOK — deploy, rollback, kill switch, latency procedures (NEW)")
    print(f"  5. AUDIT TRAIL — X-Ray at 10% sampling + full guardrail audit log\n")

    deploy_to_agentcore()


if __name__ == "__main__":
    main()
