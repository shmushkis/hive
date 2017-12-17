package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MyAbstractSplitFilter extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MyAbstractSplitFilter.class);
  
  static public MyAbstractSplitFilter SPLIT_FILTER_ABOVE_JOIN = new MyUpperJoinFilterFilter ();
  static public MyAbstractSplitFilter SPLIT_FILTER_ABOVE_CONVERTER = new MySplitFilter ();
  
  protected MyAbstractSplitFilter (RelOptRuleOperand operand) {
    super (operand);
  }

  
  static private boolean splitSupportedOperators(List<RexNode> callexpOperends,
      ArrayList<RexCall> validJdbcNode, ArrayList<RexCall> invalidJdbcNode) {
    for (RexNode currOperand : callexpOperends) {
      if ((currOperand instanceof RexCall) == false) {
        return false;
      }
      RexCall currOperandCall = (RexCall) currOperand;
      boolean isValidJdbcOp = MyJdbcRexCallValidator.isValidJdbcOperation(currOperandCall);
      if (isValidJdbcOp) {
        validJdbcNode.add(currOperandCall);
      } else {
        invalidJdbcNode.add(currOperandCall);
      }
    }
    return invalidJdbcNode.isEmpty() == false && validJdbcNode.isEmpty() == false;
  }
  
  static public boolean canSplitFilter(RexNode cond) {
    if (cond.getKind() == SqlKind.AND) {
      if (cond instanceof RexCall) {
        RexCall callExpression = (RexCall) cond;
        List<RexNode> callexpOperends = callExpression.getOperands();
        ArrayList <RexCall> validJdbcNode = new ArrayList <RexCall> ();
        ArrayList <RexCall> invalidJdbcNode = new ArrayList <RexCall> ();
        return splitSupportedOperators(callexpOperends, validJdbcNode, invalidJdbcNode);
      }
    }
    return false;
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    LOG.info("MySplitFilter.matches has been called");
    
    final HiveFilter filter = call.rel(0);
    //TODOY this is very naive imp, consult others!!!!!!
    
    RexNode cond = filter.getCondition ();

    return canSplitFilter(cond);
  }


  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MySplitFilter.onMatch has been called");
    
    final HiveFilter        filter = call.rel(0);
    //final HiveJdbcConverter converter = call.rel(1);
    //TODOY this is very naive imp, consult others!!!!!!

    RexCall callExpression = (RexCall) filter.getCondition ();
    
    List<RexNode> callexpOperends = callExpression.getOperands();
    ArrayList <RexCall> validJdbcNode = new ArrayList <RexCall> ();
    ArrayList <RexCall> invalidJdbcNode = new ArrayList <RexCall> ();
    splitSupportedOperators(callexpOperends, validJdbcNode, invalidJdbcNode);
    
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    RexNode validCondition = null;
    if (validJdbcNode.size() == 1) {
      validCondition = validJdbcNode.get(0);
    } else {
      validCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, validJdbcNode);
    }
    
    
    HiveFilter newJdbcValidFilter = new HiveFilter(filter.getCluster(), filter.getTraitSet(), filter.getInput(), validCondition);
    
    RexNode invalidCondition = null;
    if (invalidJdbcNode.size() == 1) {
      invalidCondition = invalidJdbcNode.get(0);
    } else {
      invalidCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, invalidJdbcNode);
    }
    
    
    HiveFilter newJdbcInalidFilter = new HiveFilter(filter.getCluster(), filter.getTraitSet(), newJdbcValidFilter, invalidCondition);
    
    call.transformTo(newJdbcInalidFilter);
  }
  
  
  public static class MyUpperJoinFilterFilter extends MyAbstractSplitFilter {
    public MyUpperJoinFilterFilter() {
      //super(operand(HiveFilter.class, any()));
      super(operand(HiveFilter.class,
              operand(HiveJoin.class, 
                  operand(HiveJdbcConverter.class, any()))));
    }
    
    @Override
    public boolean matches(RelOptRuleCall call) {
      LOG.info("MyUpperJoinFilterFilter.matches has been called");
      
      final HiveJoin join = call.rel(1);
      //TODOY this is very naive imp, consult others!!!!!!
      
      RexNode joinCond = join.getCondition ();

      return super.matches(call) && MyJdbcRexCallValidator.isValidJdbcOperation(joinCond);
    }
  }
  
  public static class MySplitFilter extends MyAbstractSplitFilter {
    public MySplitFilter() {
      //super(operand(HiveFilter.class, any()));
      super(operand(HiveFilter.class,
              operand(HiveJdbcConverter.class, any())));
    }
  }
  
};