import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class PreCustomer {

	public Map<Integer, String> pre() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection(Configuration.ori_url, Configuration.ori_user, Configuration.ori_pwd);
		Statement ps = con.createStatement();
		Statement ps5 = con.createStatement();
		Statement ps2 = con.createStatement();
		Statement ps3 = con.createStatement();
		Statement ps4 = con.createStatement();
		Connection con2 = DriverManager.getConnection(Configuration.cmp_url, Configuration.cmp_user, Configuration.cmp_pwd);
		PreparedStatement ps6 = con2.prepareStatement("INSERT INTO CUSTOMER_MAP(C_KEY, C_VALUE, C_PARENT, C_TYPE)VALUES(?,?,?,?)");
				
		ResultSet rs = ps.executeQuery("select distinct C_REGION from customer");
		Map<Integer, String> dateMap = new HashMap<Integer, String>();
		
		int count = 0;
		
		Integer a = 0;
		while(rs.next()) {
			a++;
			String y = rs.getString("C_REGION");
			String aa = Integer.toBinaryString(a);
			System.out.println("REGION:" + y + ", value:" + aa);
			while(aa.length()<3)
				aa = "0" + aa;
			
			ps6.setString(1, y);
			ps6.setString(2, aa);
			ps6.setString(3, null);
			ps6.setInt(4, 1);
			ps6.addBatch();
			
			ResultSet rs2 = ps2.executeQuery("select distinct C_NATION from customer where C_REGION='" + y + "' order by C_REGION");
			
			Integer b = 0;
			while(rs2.next()) {
				b++;
				String m = rs2.getString("C_NATION");
				String bb = Integer.toBinaryString(b);
				System.out.println("NATION:" + m + ", value:" + bb);
				while(bb.length()<6)
					bb = "0" + bb;
				
				ps6.setString(1, m);
				ps6.setString(2, bb);
				ps6.setString(3, aa);
				ps6.setInt(4, 2);
				ps6.addBatch();
				
				ResultSet rs3 = ps3.executeQuery("select distinct C_CITY from customer where C_REGION='" + y + "' and C_NATION='" + m + "' order by C_CITY");
				
				Integer c = 0;
				while(rs3.next()) {
					c++;
					String d = rs3.getString("C_CITY");
					String cc = Integer.toBinaryString(c);
					System.out.println("CITY:" + d + ", value:" + cc);
					while(cc.length()<6)
						cc = "0" + cc;
					
					ps6.setString(1, d);
					ps6.setString(2, cc);
					ps6.setString(3, bb);
					ps6.setInt(4, 3);
					ps6.addBatch();
					
					ResultSet rs4 = ps4.executeQuery("select distinct C_NAME, C_CUSTKEY, C_MKTSEGMENT from customer where C_REGION='" + y + "' and C_NATION='" + m + "' and C_CITY='" + d + "' order by C_NAME");
					
					Integer e = 0;
					while(rs4.next()) {
						e++;
						String name = rs4.getString("C_NAME");
						Integer id = rs4.getInt("C_CUSTKEY");
						String mktSegment = rs4.getString("C_MKTSEGMENT");
//						System.out.println(Integer.toBinaryString(a) + "," + Integer.toBinaryString(b) + "," + Integer.toBinaryString(c) + ", " + Integer.toBinaryString(e));
						
						String ee = Integer.toBinaryString(e);
						System.out.println("NAME:" + name + ", value:" + ee);
						while(ee.length() < 10)
							ee = "0" + ee;
						
						ps6.setString(1, name);
						ps6.setString(2, ee);
						ps6.setString(3, cc);
						ps6.setInt(4, 4);
						ps6.addBatch();
						
						if(++count > 1000){
							ps6.executeBatch();
							count = 0;
						}
						
//						System.out.println(aa+bb+cc+ee);
						Long newId = Long.parseLong(aa+bb+cc+ee,2);
						dateMap.put(id, aa+bb+cc+ee);
	//					ps4.executeUpdate("update DWDATE set ID="+newId+" where ID="+id);
//						ps6.execute("INSERT INTO customer()")
					}
				}
			}
		}
		
		ps6.executeBatch();
		
		con.close();
		
		return dateMap;
	
	}
}
