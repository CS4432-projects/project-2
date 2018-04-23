package simpledb.opt;

import simpledb.materialize.MergeJoinPlan;
import simpledb.parse.QueryData;
import simpledb.planner.QueryPlanner;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class ExplotSortQueryPlanner implements QueryPlanner {
   private Collection<TablePlanner> tableplanners = new ArrayList<TablePlanner>();

   private QueryData data;
   private Transaction tx;
   /**
    * Creates a query plan as follows.  It first takes
    * the product of all tables and views; it then selects on the predicate;
    * and finally it projects on the field list.
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      this.data = data;
      this.tx = tx;
      //Step 1: Create a plan for each mentioned table or view
      List<Plan> plans = new ArrayList<Plan>();
      for (String tblname : data.tables()) {
         String viewdef = SimpleDB.mdMgr().getViewDef(tblname, tx);
         if (viewdef != null)
            plans.add(SimpleDB.planner().createQueryPlan(viewdef, tx));
         else
            plans.add(new TablePlan(tblname, tx));
      }

      //Step 2: Create the product of all table plans
      Plan p = plans.remove(0);
      for (Plan nextplan : plans) {
         Plan plan = generateMergeJoinPlan(p, nextplan);
         if (plan != null) {
            p = plan;
         } else {
            p = new ProductPlan(p, nextplan);
         }
      }

      //Step 3: Add a selection plan for the predicate
      p = new SelectPlan(p, data.pred());

      //Step 4: Project on the field names
      p = new ProjectPlan(p, data.fields());
      return p;
   }

   private MergeJoinPlan generateMergeJoinPlan(Plan p1, Plan p2) {
      String fldname1 = null;
      String fldname2 = null;

      Schema sch1 = p1.schema();
      Schema sch2 = p2.schema();

      for (String fld : sch1.fields()) {
         fldname1 = fld;
         fldname2 = data.pred().equatesWithField(fld);
         if (fldname2 != null) {
            break;
         }
      }

      if (fldname1 == null || fldname2 == null) return null;

      return new MergeJoinPlan(p1, p2, fldname1, fldname2, tx);
   }
}
