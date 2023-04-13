# Assessment Grading Rubric

This assessment is split into four technical domains:
1. Data Modeling
2. Query Performance
3. Clarity and Documentation
4. dbt Familiarity

For each domain, there are a number of known areas for improvement that
candidates may identify and provide feedback.


## Technical Scoring

Candidates should be scored based on their ability to identify the known
technical issues and limitations described below. Generally speaking, stronger
candidates will be able to identify more of the items below.

**Data Modeling**

- This project is missing core entities for parts and their suppliers. It may be
  ideal to denormalize the two into a single large parts model at a
  part/supplier grain.
- The new 'lowcost_part_suppliers` model suggests that it is reflecting
  "low cost suppliers" but what this model really contains is
  "lowest cost supplier currently per part". This brings up questions of what
  "low cost" means. If low cost means below a threshold, then it may make sense
  to fine-tune how our part-supplier grain is identified.
- There's no concept of time! Admittedly, this is a consequence of the source
  data, so ymmv. It may be useful to snapshot the sources and have SCD 2 data,
  or take an immutable incremental approach.


**Query Performance**

- `lowcost_part_suppliers` breaks the "filter early" rule, and as a result may
   be a little less performant than would be useful.
- Currently, these models are all falling back on default materializations! In a
  production scenario, `lowcost_part_suppliers` ought to be materialized as a
  `table` or `incremental`

**Clarity and Documentation**

- The documentation for `lowcost_part_suppliers` could be improved to
  communicate some ways that this data set is used.
- Inconsistent naming across the board for columns. For example, `part.size` vs
  `part.part_manufacturer`

**dbt Familiarity**

- These models are missing primary key tests!
- Opportunities to use the `dbt_utils.star` macro in `lowcost_part_suppliers`

Please note that other items can and should be suggested by the candidate!
Everyone comes has had unique experiences and distinct backgrounds, so we should
expect there to be a distribution of responses from candidates of all skill
levels. The main goal is that the candidate is able to effectively communicate
the rationale for their choices.

| Items Identified | Score |
|:-----------------|:-----:|
| 9+               | 5
| 8-9              | 4
| 6-7              | 3
| 4-5              | 2
| 0-4              | 1

## Feedback Considerations
During scoring, please also be aware of _how_ the candidate provides feedback.
While not the sole focus of this interview, this is an apt opportunity to better
understand the candidate's feedback style and their ability to respect the work
of their coworkers. Ideally, the candidate is able to demonstrate providing
constructive feedback when necessary, and provided praise when deserved.

Please consult the [Commitment to Respect Each Other][CREO] if you require
guidance on what constitutes appropriate feedback.

[CREO]: https://wiki.hubspotcentral.net/display/POPS/CREO+-+Commitment+to+Respect+Each+Other
