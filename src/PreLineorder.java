import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class PreLineorder {
	public void pre(Map<Integer, String> dateMap, Map<Integer, String> customerMap) throws SQLException, ClassNotFoundException{

		Class.forName("com.mysql.jdbc.Driver");		
		Connection con = DriverManager.getConnection(Configuration.ori_url, Configuration.ori_user, Configuration.ori_pwd);
		Statement ps = con.createStatement();
		Connection con2 = DriverManager.getConnection(Configuration.cmp_url, Configuration.cmp_user, Configuration.cmp_pwd);
		
		Long begin = 0L;
		int record_size = 1000;

		while(true) {
			ResultSet rs = ps.executeQuery("select * from lineorder order by LO_ORDERKEY limit " + begin + "," + record_size);
			PreparedStatement ps2 = con2.prepareStatement("INSERT INTO C_LINEORDER(CID, LO_ORDERPRIOTITY)VALUES(?, ?)");
			boolean hasNext = false;
			
			while(rs.next()) {
				hasNext= true;
				Integer c = rs.getInt("LO_CUSTKEY");
				Integer d = rs.getInt("LO_ORDERDATE");
				Integer ln = rs.getInt("LO_LINENUMBER");
				String op = rs.getString("LO_ORDERPRIOTITY");
				
				
				String lns = Integer.toBinaryString(ln);
				if(lns.length() < 4)
					lns = "0" + lns;
				
				
				
	//			System.out.println("insert string id:" + dateMap.get(d)+customerMap.get(c) + lns);
				
				Long id = Long.parseLong(dateMap.get(d)+customerMap.get(c) + lns, 2);
				
	
	//			ps2.execute("INSERT INTO lineorder(ID, LO_ORDERPRIOTITY) VALUES (" + count + ", '" + rs.getString("LO_ORDERPRIOTITY") + "')");
				ps2.setLong(1, id);
				ps2.setString(2, op);
				ps2.addBatch();
				
			}
			ps2.executeBatch();
			if(!hasNext){				
				return;
			}
		}
	
	}
}
