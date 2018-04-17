package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnionRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCUnionPushDownRule convert a {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion}
 * into a {@link org.apache.calcite.adapter.jdbc.JdbcRules.JdbcUnion}
 * and pushes it down below the {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter}}
 * operator so it will be sent to the external table.
 */

public class JDBCUnionPushDownRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCUnionPushDownRule.class);

  final static public JDBCUnionPushDownRule INSTANCE = new JDBCUnionPushDownRule ();

  public JDBCUnionPushDownRule() {
    super(operand(HiveUnion.class,
            operand(HiveJdbcConverter.class, any()),
            operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveUnion union = call.rel(0);
    final HiveJdbcConverter converter1 = call.rel(1);
    final HiveJdbcConverter converter2 = call.rel(2);
    
    //TODO:The actual check should be the compare of the connection string of the external tables
    /*if (converter1.getJdbcConvention().equals(converter2.getJdbcConvention()) == false) {
      return false;
    }*/
    
    if (converter1.getJdbcConvention().getName().equals(converter2.getJdbcConvention().getName()) == false) {
      return false;
    }
    
    return union.getInputs().size() == 2;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("JDBCUnionPushDown has been called");
    
    final HiveUnion union = call.rel(0);
    final HiveJdbcConverter converter1 = call.rel(1);
    final HiveJdbcConverter converter2 = call.rel(2);

    final List<RelNode> unionInput = Arrays.asList(converter1.getInput(), converter2.getInput());
    Union newHiveUnion = (Union) union.copy(union.getTraitSet(), unionInput, union.all);
    JdbcUnion newJdbcUnion = (JdbcUnion) new JdbcUnionRule(converter1.getJdbcConvention()).convert(newHiveUnion);
    if (newJdbcUnion != null) {
      RelNode ConverterRes = converter1.copy(converter1.getTraitSet(), Arrays.asList(newJdbcUnion));
      
      call.transformTo(ConverterRes);
    }
  }

};