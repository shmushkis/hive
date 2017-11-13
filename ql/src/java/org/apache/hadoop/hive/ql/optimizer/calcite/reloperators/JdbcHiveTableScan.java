/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;

/**
 * Relational expression representing a scan of a HiveDB collection.
 *
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate" methods.
 * </p>
 */
public class JdbcHiveTableScan extends JdbcTableScan {

  private HiveTableScan hiveTableScan;

  public JdbcHiveTableScan(RelOptCluster cluster, RelOptTable table, JdbcTable jdbcTable,
      JdbcConvention jdbcConvention) {
    super(cluster, table, jdbcTable, jdbcConvention);
    // TODO Auto-generated constructor stub
  }
  
  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    JdbcHiveTableScan res = new JdbcHiveTableScan(
        getCluster(), table, jdbcTable, (JdbcConvention) getConvention());
    res.setHiveTableScan((HiveTableScan) this.hiveTableScan.copy(traitSet, inputs));
    //res.setHiveTableScan(this.hiveTableScan);
    return res;
  }
  
  public HiveTableScan getHiveTableScan() {
    return hiveTableScan;
  }
  
  public void setHiveTableScan(HiveTableScan hiveTableScan) {
    this.hiveTableScan = hiveTableScan;
  }
  
  public String generateSql(SqlDialect dialect) {
    final JdbcImplementor jdbcImplementor =
        new JdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final JdbcImplementor.Result result =
        jdbcImplementor.visitChild(0, this);
    return result.asStatement().toSqlString(dialect).getSql();
  }

}
