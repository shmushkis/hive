package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilterRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyFilterPushDown extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MyFilterPushDown.class);
  public MyFilterPushDown() {
    super(operand(HiveFilter.class,
        operand(HiveJdbcConverter.class, any())));
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    LOG.info("MyFilterPushDown has been called");
    
    final HiveFilter filter = call.rel(0);
    //TODOY this is very naive imp, consult others!!!!!!
    
    RexCall cond = (RexCall)filter.getCondition ();
    class MyRexVisitorImpl extends RexVisitorImpl<Void> {
      
      MyRexVisitorImpl () {
        super (true);
      }
      
      final Set<String> allowedJethroOperators = new HashSet<>(Arrays.asList("=", "<>","!=", "<",">", "sqrt","cast", "<>"));
      boolean res = true;
      
      @Override
      public Void visitCall(RexCall call) {
        LOG.info("Traversion the following RexCall:" + call + System.lineSeparator() + "with the following operator:" + call.getOperator());
        if (allowedJethroOperators.contains(call.getOperator().toString().toLowerCase()) == false) {
          res = false;
        }
        return super.visitCall(call);
      }
      
      
      public boolean go (RexCall rex) {
         rex.accept(this);
         return res;
      }

    };
    
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