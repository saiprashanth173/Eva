import unittest
from unittest import mock
from unittest.mock import MagicMock, call

from src.expression.abstract_expression import ExpressionType
from src.query_parser.eva_ql_parser_visitor import EvaParserVisitor
from src.query_parser.limit_clause import LimitClause
from third_party.evaQL.parser.frameQLParser import frameQLParser


class ParserVisitorTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def test_should_query_specification_visitor(self):
        EvaParserVisitor.visit = MagicMock()
        mock_visit = EvaParserVisitor.visit
        mock_visit.side_effect = ["target",
                                  {"from": ["from"], "where": "where"}]

        visitor = EvaParserVisitor()
        ctx = MagicMock()
        child_1 = MagicMock()
        child_1.getRuleIndex.return_value = frameQLParser.RULE_selectElements

        child_2 = MagicMock()
        child_2.getRuleIndex.return_value = frameQLParser.RULE_fromClause
        ctx.children = [None, child_1, child_2]

        expected = visitor.visitQuerySpecification(ctx)

        mock_visit.assert_has_calls([call(child_1), call(child_2)])

        self.assertEqual(expected.from_table, "from")
        self.assertEqual(expected.where_clause, "where")
        self.assertEqual(expected.target_list, "target")

    @mock.patch.object(EvaParserVisitor, 'visit')
    def test_from_clause_visitor(self, mock_visit):
        mock_visit.side_effect = ["from", "where"]

        ctx = MagicMock()
        tableSources = MagicMock()
        ctx.tableSources.return_value = tableSources
        whereExpr = MagicMock()
        ctx.whereExpr = whereExpr

        visitor = EvaParserVisitor()
        expected = visitor.visitFromClause(ctx)
        mock_visit.assert_has_calls([call(tableSources), call(whereExpr)])

        self.assertEqual(expected.get('where'), 'where')
        self.assertEqual(expected.get('from'), 'from')

    def test_logical_operator(self):
        ctx = MagicMock()
        visitor = EvaParserVisitor()

        self.assertEqual(
            visitor.visitLogicalOperator(ctx),
            ExpressionType.INVALID)

        ctx.getText.return_value = 'OR'
        self.assertEqual(
            visitor.visitLogicalOperator(ctx),
            ExpressionType.LOGICAL_OR)

        ctx.getText.return_value = 'AND'
        self.assertEqual(
            visitor.visitLogicalOperator(ctx),
            ExpressionType.LOGICAL_AND)

    def test_comparison_operator(self):
        ctx = MagicMock()
        visitor = EvaParserVisitor()

        self.assertEqual(
            visitor.visitComparisonOperator(ctx),
            ExpressionType.INVALID)

        ctx.getText.return_value = '='
        self.assertEqual(
            visitor.visitComparisonOperator(ctx),
            ExpressionType.COMPARE_EQUAL)

        ctx.getText.return_value = '<'
        self.assertEqual(
            visitor.visitComparisonOperator(ctx),
            ExpressionType.COMPARE_LESSER)

        ctx.getText.return_value = '>'
        self.assertEqual(
            visitor.visitComparisonOperator(ctx),
            ExpressionType.COMPARE_GREATER)

    @mock.patch.object(EvaParserVisitor, 'visit')
    def test_limit_clause(self, mock_visit):
        mock_visit.side_effect = lambda x: x
        ctx = MagicMock()
        visitor = EvaParserVisitor()
        ctx.limit = 10
        ctx.offset = 20
        actual = visitor.visitLimitClause(ctx)
        mock_visit.assert_has_calls([call(ctx.limit), call(ctx.offset)])
        expected = LimitClause(limit=ctx.limit, offset=ctx.offset)
        self.assertEqual(expected, actual)

    def test_decimal_literal(self):
        ctx = MagicMock()
        ctx.getText.return_value = '12'
        visitor = EvaParserVisitor()

        actual = visitor.visitDecimalLiteral(ctx)
        expected = 12.0
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
