#include "postgres.h"

#include "fmgr.h"

#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

PG_MODULE_MAGIC;

static const size_t SIMDScanMagic = 0x123456;

static set_rel_pathlist_hook_type pre_set_rel_pathlist_hook = NULL;
static create_upper_paths_hook_type pre_create_upper_paths_hook = NULL;

// scan
static CustomPathMethods zero100ScanPathMethods;
static CustomScanMethods zero100ScanMethods;
static CustomExecMethods zero100execScanMethods;

typedef struct Zero100ScanStatus {
  CustomScanState css;
  size_t SIMDScanMagic;
  int index;
  size_t size;
  int *datablock;
} Zero100ScanStatus;

static int *Zero100ScanGetDataBlock(CustomScanState *node) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;
  assert(ss->SIMDScanMagic == SIMDScanMagic);
  return ss->datablock;
}

static void Zero100ScanBeginCustomScan(CustomScanState *node, EState *estate,
                                       int eflags) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;

  ss->index = 0;
  for (int i = 0; i < ss->size; i++) {
    ss->datablock[i] = i;
  }

  return;
}

static TupleTableSlot *Zero100ScanExecCustomScan(CustomScanState *node) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;
  TupleTableSlot *ts = ss->css.ss.ss_ScanTupleSlot;

  ExecClearTuple(ts);

  if (ss->index == ss->size) {
    return NULL;
  }

  ExecStoreAllNullTuple(ts);

  ts->tts_values[0] = Int32GetDatum(ss->datablock[ss->index]);
  ts->tts_isnull[0] = false;

  ss->index++;

  return ts;
}

static void Zero100ScanEndCustomScan(CustomScanState *node) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;
  pfree(ss->datablock);
  return;
}

static Plan *_zero100_scan_create_plan(PlannerInfo *root, RelOptInfo *rel,
                                       CustomPath *best_path, List *tlist,
                                       List *clauses, List *custom_plans) {
  CustomScan *cscan = makeNode(CustomScan);

  cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
  cscan->scan.plan.targetlist = tlist;
  cscan->scan.plan.qual = NIL;
  cscan->scan.scanrelid = 0;
  cscan->custom_scan_tlist = tlist;
  cscan->methods = &zero100ScanMethods;

  return &cscan->scan.plan;
}

static Node *_zero100_scan_create_scanstate(CustomScan *cscan) {
  Zero100ScanStatus *zero100scan = palloc0(sizeof(Zero100ScanStatus));
  NodeSetTag(zero100scan, T_CustomScanState);
  zero100scan->css.flags = cscan->flags;
  zero100scan->css.methods = &zero100execScanMethods;

  zero100scan->SIMDScanMagic = SIMDScanMagic;
  zero100scan->size = 100;
  zero100scan->datablock = palloc0(sizeof(int) * zero100scan->size);

  for (int i = 0; i < zero100scan->size; i++)
    zero100scan->datablock[i] = i;

  return (Node *)zero100scan;
}

static CustomPathMethods zero100ScanPathMethods = {
    .CustomName = "Zero100ScanPath",
    .PlanCustomPath = _zero100_scan_create_plan,
};

static CustomScanMethods zero100ScanMethods = {
    .CustomName = "zero100ScanMethods",
    .CreateCustomScanState = _zero100_scan_create_scanstate,
};

static CustomExecMethods zero100execScanMethods = {
    .CustomName = "Zero100Scan",
    .BeginCustomScan = Zero100ScanBeginCustomScan,
    .ExecCustomScan = Zero100ScanExecCustomScan,
    .EndCustomScan = Zero100ScanEndCustomScan,
};

static void _set_rel_pathlist_hook_type(PlannerInfo *root, RelOptInfo *rel,
                                        Index rti, RangeTblEntry *rte) {
  if (pre_set_rel_pathlist_hook != NULL) {
    pre_set_rel_pathlist_hook(root, rel, rti, rte);
  }

  if (is_dummy_rel(rel)) {
    return;
  }

  if (rte->rtekind != RTE_RELATION) {
    return;
  }

  char *relname = get_rel_name(rte->relid);
  if (strcmp(relname, "zero100") == 0) {
    CustomPath *cpath = makeNode(CustomPath);
    cpath->path.pathtype = T_CustomScan;
    cpath->path.parent = rel;
    cpath->path.pathtarget = rel->reltarget;
    cpath->path.param_info =
        get_baserel_parampathinfo(root, rel, rel->lateral_relids);
    cpath->path.parallel_aware = false;
    cpath->path.parallel_safe = false;
    cpath->path.parallel_workers = 0;
    cpath->path.rows = 100;
    cpath->path.startup_cost = 0;
    cpath->path.total_cost = 0;
    cpath->path.pathkeys = NIL;
    cpath->flags = 0;
    cpath->custom_paths = NIL;
    cpath->custom_private = NIL;
    cpath->methods = &zero100ScanPathMethods;

    add_path(rel, &cpath->path);
  }
}

// sum

static CustomPathMethods zero100SumPathMethods;
static CustomScanMethods zero100SumMethods;
static CustomExecMethods zero100execSumMethods;

typedef struct Zero100SumStatus {
  CustomScanState css;
  int sum;
  int done;
  Node *subnode;
} Zero100SumStatus;

static void Zero100SumBeginCustomScan(CustomScanState *node, EState *estate,
                                      int eflags) {
  Zero100SumStatus *ss = (Zero100SumStatus *)node;

  ss->sum = 0;
  ss->done = 0;

  return;
}

static TupleTableSlot *Zero100SumExecCustomScan(CustomScanState *node) {
  Zero100SumStatus *ss = (Zero100SumStatus *)node;
  TupleTableSlot *ts = ss->css.ss.ss_ScanTupleSlot;

  ExecClearTuple(ts);

  if (ss->done == 1)
    return NULL;

  ExecStoreAllNullTuple(ts);

  int sum = 0;

  if (ss->subnode->type == T_CustomScanState) {
    Zero100ScanStatus *s = (Zero100ScanStatus *)ss->subnode;
    if (s->SIMDScanMagic == SIMDScanMagic) {
      for (int i = 0; i < s->size; i++) {
        sum += s->datablock[i];
      }
    }
  }

  ts->tts_values[0] = Int32GetDatum(sum);
  ts->tts_isnull[0] = false;

  ss->done = 1;

  return ts;
}

static void Zero100SumEndCustomScan(CustomScanState *node) {
  Zero100SumStatus *ss = (Zero100SumStatus *)node;
  (void)ss;
  return;
}

static Node *_zero100sum_create_scanstate(CustomScan *cscan) {
  Zero100SumStatus *ss = palloc0(sizeof(Zero100SumStatus));
  NodeSetTag(ss, T_CustomScanState);
  ss->css.flags = cscan->flags;
  ss->css.methods = &zero100execSumMethods;

  ss->done = 0;
  ss->sum = 0;

  ListCell *i;
  foreach (i, cscan->custom_plans) {
    CustomScan *cscan = lfirst(i);
    ss->subnode = (Node *)cscan->methods->CreateCustomScanState(cscan);
  }

  return (Node *)ss;
}

static Plan *_zero100_sum_create_plan(PlannerInfo *root, RelOptInfo *rel,
                                      CustomPath *best_path, List *tlist,
                                      List *clauses, List *custom_plans) {
  CustomScan *cscan = makeNode(CustomScan);

  cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
  cscan->scan.plan.targetlist = tlist;
  cscan->scan.plan.qual = NIL;
  cscan->scan.scanrelid = 0;
  cscan->custom_scan_tlist = tlist;
  cscan->methods = &zero100SumMethods;

  ListCell *i;
  foreach (i, best_path->custom_paths) {
    CustomPath *p = lfirst(i);
    cscan->custom_plans = lappend(
        cscan->custom_plans,
        p->methods->PlanCustomPath(root, rel, p, tlist, clauses, custom_plans));
  }

  return &cscan->scan.plan;
}

static void _zero100_sum_explain(CustomScanState *node, List *ancestors,
                                 ExplainState *es) {
  ExplainPropertyText("AAAAA", "BBBBB", es);

  Zero100SumStatus *ss = (Zero100SumStatus *)node;
  if (ss->subnode->type == T_CustomScanState) {
    Zero100ScanStatus *s = (Zero100ScanStatus *)ss->subnode;
    if (s->SIMDScanMagic == SIMDScanMagic) {
      ExplainPropertyText("->", "SIMD ON zero100", es);
    }
  }
}

static CustomPathMethods zero100SumPathMethods = {
    .CustomName = "Zero100SumPath",
    .PlanCustomPath = _zero100_sum_create_plan,
};

static CustomScanMethods zero100SumMethods = {
    .CustomName = "zero100SumMethods (PreAGG)",
    .CreateCustomScanState = _zero100sum_create_scanstate,
};

static CustomExecMethods zero100execSumMethods = {
    .CustomName = "zero100Sum",
    .BeginCustomScan = Zero100SumBeginCustomScan,
    .ExecCustomScan = Zero100SumExecCustomScan,
    .EndCustomScan = Zero100SumEndCustomScan,
    .ExplainCustomScan = _zero100_sum_explain,
};

static void _zero100_sum_preagg_hook(PlannerInfo *root, UpperRelationKind stage,
                                     RelOptInfo *input_rel,
                                     RelOptInfo *output_rel, void *extra) {

  if (pre_create_upper_paths_hook)
    pre_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

  if (stage != UPPERREL_GROUP_AGG)
    return;

  ListCell *ic;
  foreach (ic, output_rel->pathlist) {
    Path *j = lfirst(ic);
    if (j->type != T_AggPath) {
      continue;
    }

    AggPath *ap = (AggPath *)j;

    if (ap->subpath->type != T_CustomPath) {
      continue;
    }

    CustomPath *cp = (CustomPath *)ap->subpath;

    if (((CustomPath *)cp)->methods == &zero100ScanPathMethods) {
      CustomPath *cpath = makeNode(CustomPath);
      cpath->path.pathtype = T_CustomScan;
      cpath->path.parent = cp->path.parent;
      cpath->path.pathtarget = cp->path.pathtarget;
      cpath->path.param_info = NULL;
      cpath->path.parallel_aware = false;
      cpath->path.parallel_safe = false;
      cpath->path.parallel_workers = 0;
      cpath->path.rows = 1;
      cpath->path.startup_cost = 0;
      cpath->path.total_cost = 0;
      cpath->path.pathkeys = NIL;
      cpath->flags = 0;

      cpath->methods = &zero100SumPathMethods;
      cpath->custom_paths = list_make1(cp);
      cpath->custom_private = NIL;

      ap->subpath = (Path *)cpath;
    }

    break;
  }

  return;
}

void _PG_init(void);

void _PG_init(void) {
  pre_set_rel_pathlist_hook = set_rel_pathlist_hook;
  set_rel_pathlist_hook = _set_rel_pathlist_hook_type;

  pre_create_upper_paths_hook = create_upper_paths_hook;
  create_upper_paths_hook = _zero100_sum_preagg_hook;

  RegisterCustomScanMethods(&zero100ScanMethods);
  RegisterCustomScanMethods(&zero100SumMethods);
}
