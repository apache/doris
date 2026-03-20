// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.apache.doris.regression.suite.Suite

/**
 * Parses the MergedProfile section of a Doris query profile and returns
 * the pipeline operator tree in an explain-like format.
 *
 * Profile text structure (MergedProfile section):
 *   MergedProfile:
 *        Fragments:
 *          Fragment N:                        ← indent level A
 *            Pipeline N(instance_num=M):      ← indent level B  (B = A + 2)
 *               - WaitWorkerTime: ...         ← pipeline stats (skip)
 *              OPERATOR_NAME(...):            ← indent level C  (C = B + 2)
 *                - PlanInfo                   ← plan info marker
 *                   - key: value              ← plan info item
 *                CommonCounters:              ← counter section start (stop reading plan info)
 *                   - ...
 *
 * Usage in a test suite:
 *   // From raw profile text:
 *   def tree = profile_plan_tree(profileText)
 *   logger.info(tree)
 *
 *   // From a query_id (fetches profile via HTTP):
 *   def tree = profile_plan_tree_from_id(queryId)
 *   logger.info(tree)
 */

// ---------------------------------------------------------------------------
// profile_plan_tree(profileText) → formatted String
// ---------------------------------------------------------------------------
Suite.metaClass.profile_plan_tree = { String profileText ->

    // ── Locate MergedProfile section ──────────────────────────────────────
    def mergedStart = profileText.indexOf("MergedProfile:")
    if (mergedStart == -1) {
        return "(no MergedProfile section found in profile)"
    }
    def mergedSection = profileText.substring(mergedStart)

    // ── Line-by-line state machine ─────────────────────────────────────────
    // We detect indent levels dynamically: the first Fragment line sets the
    // baseline; Pipeline and Operator levels follow by relative indentation.
    int fragmentIndent  = -1   // indent of "Fragment N:" lines
    int pipelineIndent  = -1   // indent of "Pipeline N(...):" lines
    int operatorIndent  = -1   // indent of "OPERATOR_NAME(...):" lines
    int planInfoIndent  = -1   // indent of "- PlanInfo" lines
    int planItemIndent  = -1   // indent of "- key: value" plan info items
    int counterIndent   = -1   // indent of "CommonCounters:" / "CustomCounters:"

    // Parsed tree: list of fragments, each with pipelines, each with operators
    def fragments = []         // [{name, pipelines:[{name,instanceNum,ops:[{name,planInfo:[]}]}]}]

    def curFragName    = null
    def curPipeName    = null
    def curOpName      = null
    def curPlanInfo    = []    // accumulated plan-info lines for current operator
    def inPlanInfo     = false
    def curPipeOps     = []    // operators in current pipeline
    def curFragPipes   = []    // pipelines in current fragment

    def flushOp = {
        if (curOpName != null) {
            curPipeOps << [name: curOpName, planInfo: new ArrayList(curPlanInfo)]
            curOpName  = null
            curPlanInfo.clear()
            inPlanInfo = false
        }
    }

    def flushPipeline = {
        flushOp()
        if (curPipeName != null) {
            curFragPipes << [name: curPipeName, ops: new ArrayList(curPipeOps)]
            curPipeName = null
            curPipeOps.clear()
        }
    }

    def flushFragment = {
        flushPipeline()
        if (curFragName != null) {
            fragments << [name: curFragName, pipelines: new ArrayList(curFragPipes)]
            curFragName  = null
            curFragPipes.clear()
        }
    }

    for (def rawLine : mergedSection.split("\n")) {
        // Count leading spaces
        int spaces = 0
        for (char c : rawLine.toCharArray()) {
            if (c == ' ') spaces++
            else break
        }
        def content = rawLine.trim()
        if (content.isEmpty()) continue

        // ── Fragment ───────────────────────────────────────────────────────
        if (content =~ /^Fragment \d+:$/) {
            if (fragmentIndent == -1) fragmentIndent = spaces
            if (spaces == fragmentIndent) {
                flushFragment()
                curFragName = content[0..-2]   // strip trailing ':'
                // Reset derived indent markers when a new fragment begins
                pipelineIndent = -1
                operatorIndent = -1
                planInfoIndent = -1
                planItemIndent = -1
                counterIndent  = -1
                continue
            }
        }

        // Only process further if we are inside a Fragment
        if (curFragName == null) continue

        // ── Pipeline ───────────────────────────────────────────────────────
        if (content =~ /^Pipeline \d+\(instance_num=\d+\):$/) {
            if (pipelineIndent == -1) pipelineIndent = spaces
            if (spaces == pipelineIndent) {
                flushPipeline()
                def m = content =~ /^Pipeline (\d+)\(instance_num=(\d+)\):$/
                if (m.find()) {
                    curPipeName = "Pipeline ${m.group(1)} (instances=${m.group(2)})"
                } else {
                    curPipeName = content[0..-2]
                }
                // Reset operator-level indent markers per pipeline
                operatorIndent = -1
                planInfoIndent = -1
                planItemIndent = -1
                counterIndent  = -1
                continue
            }
        }

        // Only process further if we are inside a Pipeline
        if (curPipeName == null) continue

        // ── CommonCounters / CustomCounters ────────────────────────────────
        // These appear inside an operator block and signal end of PlanInfo.
        if (content == "CommonCounters:" || content == "CustomCounters:") {
            if (counterIndent == -1) counterIndent = spaces
            if (spaces == counterIndent) {
                inPlanInfo = false
                continue
            }
        }

        // ── PlanInfo marker ────────────────────────────────────────────────
        if (content == "- PlanInfo") {
            if (planInfoIndent == -1) planInfoIndent = spaces
            if (spaces == planInfoIndent) {
                inPlanInfo = true
                continue
            }
        }

        // ── PlanInfo item ──────────────────────────────────────────────────
        if (inPlanInfo && content.startsWith("- ")) {
            if (planItemIndent == -1) planItemIndent = spaces
            if (spaces == planItemIndent) {
                curPlanInfo << content.substring(2)   // strip leading "- "
                continue
            }
        }

        // ── Operator line ──────────────────────────────────────────────────
        // Operator names start with an uppercase letter, contain only
        // A-Z, 0-9, _, (, ) characters, and end with ':'.
        // Skip counter section headers (CommonCounters / CustomCounters already handled above).
        if (content.endsWith(":") && content =~ /^[A-Z][A-Z0-9_]/) {
            // Ignore pure counter/info headers that are not operators
            if (content == "CommonCounters:" || content == "CustomCounters:" ||
                content == "PlanInfo:") {
                continue
            }
            if (operatorIndent == -1) operatorIndent = spaces
            if (spaces == operatorIndent) {
                flushOp()
                curOpName   = content[0..-2]   // strip trailing ':'
                inPlanInfo  = false
                planInfoIndent = -1
                planItemIndent = -1
                counterIndent  = -1
                continue
            }
        }
    }

    flushFragment()

    // ── Format output ──────────────────────────────────────────────────────
    // Similar to explain plan:
    //   Fragment N:
    //     Pipeline M (instances=K):
    //       OPERATOR_NAME (...)
    //         | plan-info-key: value
    //         | ...
    if (fragments.isEmpty()) {
        return "(MergedProfile found but no fragments could be parsed)"
    }

    def sb = new StringBuilder()
    for (def frag : fragments) {
        sb.append("${frag.name}:\n")
        for (def pipe : frag.pipelines) {
            sb.append("  ${pipe.name}:\n")
            for (def op : pipe.ops) {
                sb.append("    ${op.name}\n")
                for (def pi : op.planInfo) {
                    sb.append("      | ${pi}\n")
                }
            }
        }
    }

    return sb.toString()
}

// ---------------------------------------------------------------------------
// profile_plan_tree_from_id(queryId) → formatted String
//   Fetches profile via HTTP then calls profile_plan_tree.
// ---------------------------------------------------------------------------
Suite.metaClass.profile_plan_tree_from_id = { String queryId ->
    Suite suite = delegate as Suite
    def dst  = 'http://' + suite.context.config.feHttpAddress
    def conn = new URL("${dst}/api/profile/text?query_id=${queryId}").openConnection()
    conn.setRequestMethod("GET")
    def user     = suite.context.config.feHttpUser     ?: "root"
    def pass     = suite.context.config.feHttpPassword ?: ""
    def encoding = Base64.getEncoder().encodeToString("${user}:${pass}".getBytes("UTF-8"))
    conn.setRequestProperty("Authorization", "Basic ${encoding}")
    conn.setConnectTimeout(5000)
    conn.setReadTimeout(15000)
    def profileText = conn.getInputStream().getText()
    return suite.profile_plan_tree(profileText)
}

logger.info("Added 'profile_plan_tree' and 'profile_plan_tree_from_id' to Suite")
