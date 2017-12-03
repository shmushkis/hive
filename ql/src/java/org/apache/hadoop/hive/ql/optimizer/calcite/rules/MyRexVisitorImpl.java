package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MyRexVisitorImpl extends RexVisitorImpl<Void> {
  static Logger LOG = LoggerFactory.getLogger(MyFilterPushDown.class);
  
  static class JethroOperatorsPredicate {
    final static Set<String> allowedJethroOperators = new HashSet<>(Arrays.asList("=", "<>","!=", "<",">", "sqrt",
                                            "cast", "<>", "+", "-", "*", "/", "is not null", "and", "or", "not", "cast"));
    
    static boolean isValidOperator (SqlOperator operator, RelDataType type, ArrayList<RelDataType> paramsList) {
      if (allowedJethroOperators.contains(operator.toString().toLowerCase())) {
        return true;
      }
      LOG.debug("JETHRO: Skipped push down for " + operator.toString() + ", due to the following unsupporetd function" + operator.toString());
      return false;
    }
    
    
    public static boolean validRexCall (RexCall call) {
      final SqlOperator operator = call.getOperator();
      SqlKind kind = call.getKind();
      List <RexNode> operands = call.getOperands();
      RelDataType resType = call.getType();
      ArrayList<RelDataType> paramsListType = new ArrayList<RelDataType>();
      for (RexNode currNode : operands) {
        paramsListType.add(currNode.getType());
      }
      return isValidOperator(operator, resType, paramsListType);
    }
  }
  
  
  MyRexVisitorImpl () {
    super (true);
  }
  
  boolean res = true;
  
  @Override
  public Void visitCall(RexCall call) {
    LOG.info("JETHRO: Traversion the following RexCall:" + call + System.lineSeparator() + "with the following operator:" + call.getOperator());
    boolean isValidCall = JethroOperatorsPredicate.validRexCall (call);
    res &= isValidCall;
    return super.visitCall(call);
  }
  
  
  public boolean go (RexNode cond) {
     cond.accept(this);
     return res;
  }

};
