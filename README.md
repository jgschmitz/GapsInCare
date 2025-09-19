Flat to MongoDB Document Model

Transform a flat, column-per-attribute dataset (your Spark StructType) into a clean, query-friendly MongoDB document model.

Table of Contents

Why reshape

Target identity

Schema overview

Field mapping

Type conversion rules

Example document

Transformation steps

Writing to MongoDB

Indexes

Validation checklist

Why reshape

The flat layout scatters related attributes, making queries and indexes awkward.

Many fields are string-typed even when they are flags, numbers, or dates.

Grouping related attributes (member, gap, HCC, labs, rules) as subdocuments and arrays matches read patterns.

Target identity

Use a deterministic _id so upserts are idempotent.

_id = "{GLB_MBR_ID}:{GAP_ID}:{SUS_YEAR}"


If you have a guaranteed unique, stable key such as UNIQ_MBR_GAP_ID, you can use that instead.

Schema overview

Top-level groups in the target document:

member — ids, age, gender, years of service

client — CLI identifiers

status — active/ignore/disenrollment flags

engine — provenance (dates, source, run ids)

gap — id, description, source, status, rank, probabilities; optional gap.sub

dssCategory

hcc

encounter — start/end dates

labs — counts, DOS, LOINC codes/types/results (arrays)

manifestation

related — dx and cpt subdocs

rx

provider

recommendation

rules

coverage

counts

flags

sourceFile

suspect

likelihood, ttlProb (top-level numbers)

Field naming tip: prefer lowerCamelCase for MongoDB fields.

Field mapping

Examples of how flat columns map to the document:

Flat column(s)	Target field
GLB_MBR_ID, MBR_ID, MBR_AGE, GENDER, YOS	member.globalId, member.memberId, member.age, member.gender, member.yearsOfService
CLI_ID, CLI_SK, SUB_CLI_SK, D_SUB_CLI_SK, CLNT_GU_ID	client.*
ACTIVE_FLAG, DISENROLLMENT_FLAG, IGNORE, IGNORE_REASON, STS_CHG_IND	status.*
ENGINE_DATE, ENGINE_SOURCE, ENGINE_TRIGGEREVENT, ENGINE_GAP_ID, BATCH_ID, RUN_ID	engine.*
GAP_*, SUB_GAP_*	gap.*, gap.sub.*
DSS_CTGY*	dssCategory.*
HCC*, SEC_HCC, HIERARCHICAL_STATUS	hcc.*
ENCOUNTER_START_DATE, ENCOUNTER_END_DATE	encounter.start, encounter.end
LAB_*	labs.* with arrays for LOINC/types/results
RELDX_*, RELCPT_*, RELATE_*	related.dx.*, related.cpt.*
RX_*, NDC, NDC_DESC	rx.*
PROVIDER_ID	provider.id
RECOM_*	recommendation.*
RULE_*, PAR_RULE_*	rules.*
MDL_*, IND_STD_MDL_CD, METAL_LEVEL	coverage.*
*_CNT, factors	counts.*
Remaining flags	flags.*
SRC_FL_*, SRC_SPCT_ID	sourceFile.*
SUSPECT_PERIOD, SUS_YEAR	suspect.*
LIKELIHOOD, TTL_PROB	likelihood, ttlProb
Type conversion rules

Booleans: columns ending with _FLAG and any Y/N, 1/0, TRUE/FALSE strings become booleans.

Numbers: counts (*_CNT), ranks (*_RNK), factors (*_FCT), and probabilities (LIKELIHOOD, TTL_PROB) become numeric types (int or double).

Dates: ENCOUNTER_*, SRC_FL_DT, ENGINE_DATE, LAB_DOS become ISODate.

IDs and codes: keep as strings when leading zeros matter (member ids, NDC, LOINC, ICD/HCC, GAP_ID, provider id).

Arrays: fold numbered fields into arrays (for example LAB_LOINC_CD1/2 → labs.loinc).
