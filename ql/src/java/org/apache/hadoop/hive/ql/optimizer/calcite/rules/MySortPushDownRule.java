package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSort;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSortRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySortPushDownRule extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MySortPushDownRule.class);
  
  public MySortPushDownRule() {
    super(operand(HiveSortLimit.class,
        operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.info("MySortPushDownRule has been called");
    
    final HiveSortLimit sort = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    //TODOY this is very naive imp, consult others!!!!!!
    
    Sort newHiveSort = sort.copy(sort.getTraitSet(), converter.getInput(),sort.getCollation() ,sort.getOffsetExpr (),sort.getFetchExpr());
    JdbcSort newJdbcSort = (JdbcSort) new JdbcSortRule(JdbcConvention.JETHRO_DEFAULT_CONVENTION).convert(newHiveSort);
    RelNode ConverterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcSort));


    call.transformTo(ConverterRes);
  }
  
};