"""
This is a test end-end runner for EVA. For now, it only takes care of the
execution engine while mocking the query optimizer.

Note: This is only for demo purpose. This commit will be reverted!
"""
import os
from typing import Optional

from src.expression.abstract_expression import AbstractExpression
from src.expression.constant_value_expression import ConstantValueExpression
from src.expression.function_expression import FunctionExpression, \
    ExecutionMode
from src.expression.tuple_value_expression import TupleValueExpression
from src.models.catalog.properties import VideoFormat
from src.models.catalog.video_info import VideoMetaInfo
from src.query_executor.plan_executor import PlanExecutor
from src.query_parser.eva_parser import EvaFrameQLParser
from src.query_parser.select_statement import SelectStatement
from src.query_parser.table_ref import TableRef
from src.query_planner.abstract_plan import AbstractPlan
from src.query_planner.seq_scan_plan import SeqScanPlan
from src.query_planner.storage_plan import StoragePlan
from src.udfs.fastrcnn_object_detector import FastRCNNObjectDetector

UDFS = {
    "fastrcnn": FastRCNNObjectDetector
}

VIDEOS = {
    "test_1": VideoMetaInfo(
        "/Users/prashanth/Projects/DDL/Eva/data/test_1.mp4", 30,
        VideoFormat.MP4)

}

DEVICE = os.environ.get('DEVICE', 'cpu')


def bind_update_where_clause(clause: AbstractExpression) -> Optional[
    AbstractExpression]:
    """
    Update the expression types to suite the plan tree!

    Arguments:
        clause (AbstractExpression): an expression tree.
    Returns:
        AbstractExpression: Updated expression tree
    """
    if clause is None:
        return
    if isinstance(clause, TupleValueExpression):
        col_lower = clause._col_name.lower()
        if col_lower in UDFS:
            return FunctionExpression(UDFS[col_lower]().to(DEVICE),
                                      mode=ExecutionMode.EXEC, name=col_lower)
    if isinstance(clause, ConstantValueExpression):
        constant = clause.evaluate().lower().replace("'", "")
        return ConstantValueExpression(constant
                                       )
    for child_idx in range(clause.get_children_count()):
        clause.update_child(child_idx, bind_update_where_clause(
            clause.get_child(child_idx)))

    return clause


def bind_table_ref(table_ref: TableRef) -> VideoMetaInfo:
    """
    Used for binding the table references with information required for
    loading frames from the disk. This information comes from the catalog.

    Arguments:
        table_ref (TableRef): the table reference object obtained from the
        parser. Used form constructing the VideoMetaInfo from catalog.

    Returns:
        VideoMetaInfo: the meta-info obtained from catalog.

    """
    return VIDEOS[table_ref.table_info.table_name.lower()]


def build_plan_tree(stmt: SelectStatement) -> AbstractPlan:
    """
    Use to build the physical plan tree.

    Arguments:
        stmt (SelectStatement): the select statement for which the plan
        needs to be built.

    Returns:
        AbstractPlan: the built abstract



    """
    limit = None
    offset = None
    if stmt.limit_clause:
        limit = stmt.limit_clause.limit
        offset = stmt.limit_clause.offset

    plan = SeqScanPlan(bind_update_where_clause(stmt.where_clause))
    storage = StoragePlan(bind_table_ref(stmt.from_table), batch_size=5,
                          limit=limit, offset=offset)
    plan.append_child(storage)
    return plan


def driver(sql):
    """
    The main driver code for EVA.
    """
    sql = sql.upper()
    parser = EvaFrameQLParser()
    stmt = parser.parse(sql)
    plan = build_plan_tree(stmt[0])

    executor = PlanExecutor(plan)

    return executor.execute_plan()


if __name__ == "__main__":
    query = input("Query: ")
    result = driver(query)
    print(result)
