package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class ExtensibleHashIndex implements Index {
	public static int MAX_ITEMS_BUCKET = 5;
	private String idxname;
	private Schema sch;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	private TableScan globalscan = null;
	private TableInfo globalTable;
	private int globalDepth;
	private int localDepth;
	private String bucketID;

	/**
	 * Opens a hash index for the specified index.
	 * @param idxname the name of the index
	 * @param sch the schema of the index records
	 * @param tx the calling transaction
	 */
	public ExtensibleHashIndex(String idxname, Schema sch, Transaction tx) {
		this.idxname = idxname;
		this.sch = sch;
		this.tx = tx;

		String globalname = idxname + "global";
		globalTable = new TableInfo(globalname, globalSchema());
		globalscan = new TableScan(globalTable, tx);
		globalscan.beforeFirst();
		if(!globalscan.getString("id").equals("global")) {
			globalscan.insert();
			globalscan.setString("id", "global");
			globalscan.setInt("depth", 1);

			globalscan.insert();
			globalscan.setString("id", "0");
			globalscan.setInt("depth", 1);
			globalscan.setString("filename", "exh"+idxname+"0");

			globalscan.insert();
			globalscan.setString("id", "1");
			globalscan.setInt("depth", 1);
			globalscan.setString("filename", "exh"+idxname+"1");

			globalDepth = 1;
		} else {
			globalDepth = globalscan.getInt("depth");
		}
	}

	/**
	 * Positions the index before the first index record
	 * having the specified search key.
	 * The method hashes the search key to determine the bucket,
	 * and then opens a table scan on the file
	 * corresponding to the bucket.
	 * The table scan for the previous bucket (if any) is closed.
	 * @see Index#beforeFirst(Constant)
	 */
	public void beforeFirst(Constant searchkey) {
		close();
		String binaryHashCode = Integer.toBinaryString(searchkey.hashCode());
		bucketID = binaryHashCode.substring(binaryHashCode.length() - globalDepth);
		// actually open the correct bucket for the given searchkey
		globalscan = new TableScan(globalTable, tx);
		globalscan.beforeFirst();
		String bucketFile = "";
		while(globalscan.next()) {
			if (globalscan.getString("id").equals(bucketID)) {
				bucketFile = globalscan.getString("filename");
				break;
			}
		}
		this.searchkey = searchkey;
		TableInfo ti = new TableInfo(bucketFile, sch);
		ts = new TableScan(ti, tx);
	}

	/**
	 * Moves to the next record having the search key.
	 * The method loops through the table scan for the bucket,
	 * looking for a matching record, and returning false
	 * if there are no more such records.
	 * @see Index#next()
	 */
	public boolean next() {
		while (ts.next())
			if (ts.getVal("dataval").equals(searchkey))
				return true;
		return false;
	}

	/**
	 * Retrieves the dataRID from the current record
	 * in the table scan for the bucket.
	 * @see Index#getDataRid()
	 */
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}

	/**
	 * Inserts a new record into the table scan for the bucket.
	 * @see Index#insert(Constant, RID)
	 */
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		ts.beforeFirst();
		int numRecords = 0;
		while(ts.next()) {
			numRecords++;
		}
		if (numRecords >= MAX_ITEMS_BUCKET) {
			increaseLocalDepth();
			insert(val, rid);
			// we need to increase the local depth and do a bunch of stuff
		} else {
			ts.insert();
			ts.setInt("block", rid.blockNumber());
			ts.setInt("id", rid.id());
			ts.setVal("dataval", val);
		}
	}

	private void increaseLocalDepth() {

		ts.beforeFirst();
		while(ts.next()) {
			ts.
		}
	}

	private void increaseGlobalDepth() {
		globalscan = new TableScan(globalTable, tx);
		TableScan newGlobalTable = new TableScan(new TableInfo(idxname+"globalnew", globalSchema()), tx);
		newGlobalTable.beforeFirst();
		newGlobalTable.setString("id", "global");
		newGlobalTable.setInt("depth", globalDepth+1);

		globalscan.beforeFirst();
		globalscan.next();
		while(globalscan.next()) {
			String id = globalscan.getString("id");
			String filename = globalscan.getString("filename");
			int depth = globalscan.getInt("depth");

			newGlobalTable.insert();
			newGlobalTable.setString("id", "0"+id);
			newGlobalTable.setString("filename", filename);
			newGlobalTable.setInt("depth", depth);

			newGlobalTable.insert();
			newGlobalTable.setString("id", "1"+id);
			newGlobalTable.setString("filename", filename);
			newGlobalTable.setInt("depth", depth);
		}

		
	}

	/**
	 * Deletes the specified record from the table scan for
	 * the bucket.  The method starts at the beginning of the
	 * scan, and loops through the records until the
	 * specified record is found.
	 * @see Index#delete(Constant, RID)
	 */
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next())
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
	}

	/**
	 * Closes the index by closing the current table scan.
	 * @see Index#close()
	 */
	public void close() {
		if (ts != null)
			ts.close();
		if (globalscan != null)
			globalscan.close();
	}


	private Schema globalSchema() {
		Schema sch = new Schema();
		sch.addStringField("id", 20);
		sch.addStringField("filename", 40);
		sch.addIntField("depth");
		return sch;
	}
}
