package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * Rule that tries to push filter expressions into a join condition and into
 * the inputs of the join.
 */

public class JDBCFilterJoinRule extends HiveFilterJoinRule {
  
  final static public JDBCFilterJoinRule INSTANCE = new JDBCFilterJoinRule ();
  
  
  public JDBCFilterJoinRule() {
    super(RelOptRule.operand(HiveFilter.class, 
            RelOptRule.operand(HiveJoin.class, 
                RelOptRule.operand(HiveJdbcConverter.class, RelOptRule.any()),
                RelOptRule.operand(HiveJdbcConverter.class, RelOptRule.any()))),
        "JDBCFilterJoinRule", true, HiveRelFactories.HIVE_BUILDER);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join   join = call.rel(1);
    HiveJdbcConverter   conv1 = call.rel(2);
    HiveJdbcConverter   conv2 = call.rel(3);
    
    if (conv1.getJdbcDialect().equals(conv2.getJdbcDialect()) == false) {
      return false;
    }
    
    boolean visitorRes = JDBCRexCallValidator.isValidJdbcOperation(filter.getCondition(),conv1.getJdbcDialect());
    if (visitorRes) {
      return JDBCRexCallValidator.isValidJdbcOperation(join.getCondition(), conv1.getJdbcDialect());  
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join join = call.rel(1);
    super.perform(call, filter, join);
  }
}
