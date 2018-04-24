package simpledb.index.hash;

import simpledb.index.Index;
import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.util.ArrayList;

/**
 * A static hash implementation of the Index interface.
 * A fixed number of buckets is allocated (currently, 100),
 * and each bucket is implemented as a file of index records.
 * @author Edward Sciore
 */
public class ExtensibleHashIndex implements Index {
	public static int MAX_ITEMS_BUCKET = 5; // //CS4432-Project2: the max items in an extensible hash bucket
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
		//CS4432-Project2: the global table for this index
		String globalname = idxname + "global";
		globalTable = new TableInfo(globalname, globalSchema());
		globalscan = new TableScan(globalTable, tx);
		globalscan.beforeFirst();
		globalscan.next();
		try {
			if (globalscan.getString("id").equals("global")) {
				globalDepth = globalscan.getInt("depth");
			}
		} catch(IllegalArgumentException e) {
			//CS4432-Project2: the first time we have created the global index, depth = 1
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
		binaryHashCode = String.format("%32s", binaryHashCode).replace(' ', '0'); // pad binary with zeros
		System.out.println("BinaryHashCode = :" + binaryHashCode);
		//CS4432-Project2: calculate the bucket id using the globalDepth
		bucketID = binaryHashCode.substring(binaryHashCode.length() - globalDepth);
		globalscan = new TableScan(globalTable, tx);
		globalscan.beforeFirst();
		String bucketFile = "";
		//CS4432-Project2: Search for the bucket id in the global table
		while(globalscan.next()) {
			if (globalscan.getString("id").equals(bucketID)) {
				bucketFile = globalscan.getString("filename");
				localDepth = globalscan.getInt("depth");
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
		//CS4432-Project2: count the records in the bucket
		while(ts.next()) {
			numRecords++;
		}
		//CS4432-Project2: increase the localDepth if necessary
		if (numRecords >= MAX_ITEMS_BUCKET) {
			increaseLocalDepth();
			insert(val, rid);
		} else {
			ts.insert();
			ts.setInt("block", rid.blockNumber());
			ts.setInt("id", rid.id());
			ts.setVal("dataval", val);
		}
	}

	private void increaseLocalDepth() {
		ArrayList<BucketContents> oldContents = new ArrayList<>();
		ts.beforeFirst();
		//CS4432-Project2: Save the old contents
		while(ts.next()) {
			oldContents.add(new BucketContents(ts.getInt("block"), ts.getInt("id"), ts.getVal("dataval")));
		}

		// check if we need to increase the globalDepth
		if (localDepth + 1 <= globalDepth) {
			// just increase the localDepth
			globalscan = new TableScan(globalTable, tx);
			globalscan.beforeFirst();
			globalscan.next(); // skip the first entry that holds the globaldepth
			while(globalscan.next()) {
				String binaryHashCode = Integer.toBinaryString(searchkey.hashCode());
				binaryHashCode = String.format("%32s", binaryHashCode).replace(' ', '0'); // pad binary with zeros
				String localBits = binaryHashCode.substring(binaryHashCode.length() - localDepth);
				String globalid = globalscan.getString("id");

				if (globalid.endsWith(localBits)) {
					globalscan.setInt("depth", localDepth + 1);
					System.out.println("The global id is :" + globalid);
					System.out.println("THe global depth is :" + globalDepth);
					System.out.println("The localdepth is :" + localDepth);
					String newGlobalBits = globalid.substring(globalid.length() - (localDepth + 1));
					globalscan.setString("filename", "exh"+idxname+newGlobalBits);
				}
			}
			localDepth++;
			for (BucketContents contents : oldContents) {
				insert(contents.val, new RID(contents.block, contents.id));
			}
		} else {
			increaseGlobalDepth();
			increaseLocalDepth();
		}

	}

	//CS4432-Project2: data class for storing bucket info
	private class BucketContents {
		public int block;
		public int id;
		public Constant val;

		public BucketContents(int block, int id, Constant val) {
			this.block = block;
			this.id = id;
			this.val = val;
		}
	}

	private void increaseGlobalDepth() {
		//CS4432-Project2: this copies all of the old data into a temporary file and then increases the values.
		// It then deletes everything in the old global table and copies the new values back.

		globalscan = new TableScan(globalTable, tx);
		TableScan newGlobalTable = new TableScan(new TableInfo(idxname+"globalnew", globalSchema()), tx);
		newGlobalTable.beforeFirst();
		newGlobalTable.insert();
		newGlobalTable.setString("id", "global");
		newGlobalTable.setInt("depth", ++globalDepth);

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

		globalscan.beforeFirst();
		while(globalscan.next()) {
			globalscan.delete();
		}
		newGlobalTable.beforeFirst();
		while(newGlobalTable.next()) {
			String id = newGlobalTable.getString("id");
			String filename = newGlobalTable.getString("filename");
			int depth = newGlobalTable.getInt("depth");
			globalscan.insert();
			globalscan.setString("id", id);
			globalscan.setString("filename", filename);
			globalscan.setInt("depth", depth);
		}

		newGlobalTable.beforeFirst();
		while(newGlobalTable.next()) {
			newGlobalTable.delete();
		}
		newGlobalTable.close();


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
		//CS4432-Project2: the schema used for the global table
		Schema sch = new Schema();
		sch.addStringField("id", 20);
		sch.addStringField("filename", 40);
		sch.addIntField("depth");
		return sch;
	}
}
