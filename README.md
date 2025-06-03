# Gaps In Care 
SQL to MongoDB Schema recommendation walkthrough

What can be improved over `oldschema.txt` with this schema (from a MongoDB or modern modeling lens)?<br>
🚫 Over-flattened structure
Everything is a top-level string column — even nested, related concepts like:
Lab results - 
Rules and recommendations - 
Gaps and sub-gaps - 

Encounters <br>
🚫 Implied relationships, not explicit
Things like RULE_ID, SUB_GAP_ID, CLI_SK, GAP_ID suggest foreign key behavior, but that’s lost in this format.

It becomes hard to do flexible queries across related concepts or histories without joining 100+ columns.

🚫 Opaque column names
SPRS, SPRS_CD, TTL_PROB, YOS… you need a glossary just to write a query.
Makes semantic search and intelligent filtering almost impossible.

🚫 Homogenized typing
Everything is a StringType() — even dates, flags, numerics.
Suggests the data probably came through a Spark pipeline or was ingested without enforced typing.

MongoDB lets you represent the business objects as actual documents, not as 150+ disconnected fields. For example:

``` javascipt
{
  "memberId": "MBR_001",
  "gender": "F",
  "age": 67,
  "globalMemberId": "GLB_001",
  "hicNumber": "HIC_12345",

  "gap": {
    "gapId": "GAP123",
    "description": "No follow-up after abnormal A1C",
    "status": "Open",
    "source": "CDI",
    "reason": "Patient did not schedule follow-up",
    "statusChangeReason": "Contact attempt failed",
    "category": "Chronic Condition",
    "keywords": ["A1C", "follow-up"],
    "rank": 1,
    "likelihood": "High",

    "subGaps": [
      {
        "subGapId": "SUBGAP_1",
        "reason": "Lab flagged as abnormal",
        "rank": 1,
        "status": "Pending"
      }
    ],

    "suppression": {
      "isSuppressed": true,
      "code": "OPT_OUT",
      "reason": "Member opted out",
      "source": "System",
      "timestamp": "2024-12-01T10:00:00Z"
    }
  },

  "lab": {
    "dateOfService": "2024-11-15",
    "loincCode": "4548-4",
    "testType": "Hemoglobin A1C",
    "result": "9.2"
  },

  "recommendation": {
    "source": "RulesEngine",
    "type": "Follow-up Appointment",
    "text": "Schedule visit with endocrinologist",
    "value": "High Priority"
  },

  "rules": {
    "ruleId": "R123",
    "ruleVersion": "1.2.3",
    "parentRuleId": "PR001",
    "shortName": "A1C Follow-Up",
    "hierarchy": {
      "group": "ChronicCare",
      "level": "Tier2"
    }
  },

  "engine": {
    "runId": "RUN202405",
    "executionDate": "2024-12-01T08:00:00Z",
    "triggerEvent": "LabResult",
    "source": "Catena"
  }
}
```
Why This MongoDB Schema Works Better?

| Field                          | Index Type | Why It Matters                                                                                                                                                                                                                                                                                  |
|-------------------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `memberId`, `gap.status`      | Compound   | Optimized for fetching *open gaps per member*, a common care coordination task. Filters efficiently by patient ID and gap status — e.g., "Show me all open care gaps for this member."                                                                                                          |
| `gap.suppression.isSuppressed`| Single     | Supports fast toggling between *suppressed* vs. *active* gaps. Essential for filtering out ignored or deferred gaps without scanning the whole document set.                                                                                                                                    |
| `lab.loincCode`, `lab.result` | Compound   | Enables fast lookups on *lab-based triggers*. Useful for identifying lab results (like HbA1c) that open or close gaps. Queries typically require both code and result value.                                                                                                                    |
| `engine.runId`                | Single     | Tracks which *engine run* generated each gap — useful for debugging, rollback, or batch analysis like "What gaps came from May 2025's run?"                                                                                                               |
| `gap.subGaps.subGapId`        | Multikey   | Supports targeted querying of *nested sub-gaps*, such as detailed quality measures (e.g., colonoscopy prep + follow-up). The multikey index ensures performance holds even when sub-gaps vary across documents.                                           |





