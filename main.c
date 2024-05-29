#include "postgres.h"

#include "executor/tuptable.h"
#include "fmgr.h"
#include "nodes/extensible.h"
#include "nodes/parsenodes.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"

#include <assert.h>
#include <string.h>

PG_MODULE_MAGIC;

static const size_t SIMDScanMagic = 0x123456;

static set_rel_pathlist_hook_type pre_set_rel_pathlist_hook = NULL;

typedef struct Zero100ScanStatus {
  CustomScanState css;
  size_t SIMDScanMagic;
  int index;
  size_t size;
  int *datablock;
} Zero100ScanStatus;

static int *Zero100GetDataBlock(CustomScanState *node) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;
  assert(ss->SIMDScanMagic == SIMDScanMagic);
  return ss->datablock;
}

static void Zero100BeginCustomScan(CustomScanState *node, EState *estate,
                                   int eflags) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;

  ss->index = 0;
  for (int i = 0; i < ss->size; i++) {
    ss->datablock[i] = i;
  }

  return;
}

static TupleTableSlot *Zero100ExecCustomScan(CustomScanState *node) {
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

static void Zero100EndCustomScan(CustomScanState *node) {
  Zero100ScanStatus *ss = (Zero100ScanStatus *)node;
  pfree(ss->datablock);
  return;
}

static CustomExecMethods zero100execScanMethods = {
    .CustomName = "Zero100",
    .BeginCustomScan = Zero100BeginCustomScan,
    .ExecCustomScan = Zero100ExecCustomScan,
    .EndCustomScan = Zero100EndCustomScan,
};

static CustomScanMethods zero100ScanMethods;

static Plan *_zero100_create_plan(PlannerInfo *root, RelOptInfo *rel,
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

static CustomPathMethods zero100PathMethods = {
    .CustomName = "Zero100",
    .PlanCustomPath = _zero100_create_plan,
    .ReparameterizeCustomPathByChild = NULL,
};

static Node *_zero100_create_scanstate(CustomScan *cscan) {
  Zero100ScanStatus *zero100scan = palloc0(sizeof(Zero100ScanStatus));
  NodeSetTag(zero100scan, T_CustomScanState);
  zero100scan->css.flags = cscan->flags;
  zero100scan->css.methods = &zero100execScanMethods;

  zero100scan->SIMDScanMagic = SIMDScanMagic;
  zero100scan->size = 100;
  zero100scan->datablock = palloc0(sizeof(int) * zero100scan->size);

  // 用 cscan->custom_private 找相邻的同属于自己的节点. 选高效的传递方式

  return (Node *)zero100scan;
}

static CustomScanMethods zero100ScanMethods = {
    .CustomName = "Zero100",
    .CreateCustomScanState = _zero100_create_scanstate,
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

  // PlannerInfo 规划器 （不知道那些字段可用
  // RelOptInfo
  // Index
  // RangeTblEntry TangeTableEntry 表

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
    cpath->methods = &zero100PathMethods;

    add_path(rel, &cpath->path);
  }
}

void _PG_init(void);

void _PG_init(void) {
  pre_set_rel_pathlist_hook = set_rel_pathlist_hook;
  set_rel_pathlist_hook = _set_rel_pathlist_hook_type;

  RegisterCustomScanMethods(&zero100ScanMethods);
}

/*
RegisterCustomScanMethods 给 parseNodeString 用的，由 nodeToString 写出。
ExecSerializePlan 路径用，worker 相关的


planner_hook; 代替 standard_planner
create_upper_paths_hook;

post_parse_analyze_hook; 解析器部分

set_rel_pathlist_hook_type 增加一个 path 到表
set_join_pathlist_hook_type 增加一个 path 到 join

PlanCustomPath 把 Path 转成 Plan
ReparameterizeCustomPathByChild

CreateCustomScanState 吧 Plan 转成 ScanState，来执行

BeginCustomScan
ExecCustomScan
EndCustomScan
ReScanCustomScan
执行
*/
