package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.sql.SqlDialect;

public class HiveJdbcConverter extends ConverterImpl implements HiveRelNode {

  public HiveJdbcConverter(RelOptCluster cluster,  RelTraitSet traits,
      JdbcRel input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void implement(Implementor implementor) {
    // TODO Auto-generated method stub

  }
  
  public String generateSql(SqlDialect dialect) {
    final JdbcImplementor jdbcImplementor =
        new JdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final JdbcImplementor.Result result =
        jdbcImplementor.visitChild(0, getInput());
    return result.asStatement().toSqlString(dialect).getSql();
  }

}
