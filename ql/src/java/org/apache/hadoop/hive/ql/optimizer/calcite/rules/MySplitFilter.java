package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilterRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySplitFilter extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MySplitFilter.class);
  
  public MySplitFilter() {
    super(operand(HiveFilter.class,
        operand(HiveJdbcConverter.class, any())));
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    LOG.info("MyFilterPushDown has been called");
    
    final HiveFilter filter = call.rel(0);
    //TODOY this is very naive imp, consult others!!!!!!
    
    RexNode cond = filter.getCondition ();

    if (cond.getKind() == SqlKind.AND) {
      if (cond instanceof RexCall) {
        RexCall callExpression = (RexCall) cond;
        List<RexNode> operends = callExpression.getOperands();
        ArrayList <RexCall> validJdbcNode = new ArrayList <RexCall> ();
        ArrayList <RexCall> invalidJdbcNode = new ArrayList <RexCall> ();
        for (RelOptRuleOperand currOperand : operands) {
          //if (new MyRexVisitorImpl ().go(currOperand)) {
          //  
          //}
        }
      }
    }
    
    boolean visitorRes = new MyRexVisitorImpl ().go(cond);
    
    return visitorRes;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.info("MyFilterPushDown has been called");
    
    final HiveFilter filter = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    //TODOY this is very naive imp, consult others!!!!!!
    
    Filter newHiveFilter = filter.copy(filter.getTraitSet(), converter.getInput(),filter.getCondition());
    JdbcFilter newJdbcFilter = (JdbcFilter) new JdbcFilterRule(JdbcConvention.JETHRO_DEFAULT_CONVENTION).convert(newHiveFilter);
    RelNode ConverterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcFilter));
    
    
    call.transformTo(ConverterRes);
  }
  
};