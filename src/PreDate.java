import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class PreDate {

	public Map<Integer, String> pre() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection(Configuration.ori_url, Configuration.ori_user, Configuration.ori_pwd);
		Statement ps = con.createStatement();
		Statement ps5 = con.createStatement();
		Statement ps2 = con.createStatement();
		Statement ps3 = con.createStatement();
		Statement ps4 = con.createStatement();
		Connection con2 = DriverManager.getConnection(Configuration.cmp_url, Configuration.cmp_user, Configuration.cmp_pwd);
		Statement ps6 = con2.createStatement();
		PreparedStatement ps7 = con2.prepareStatement("INSERT INTO CD_DATE(CD_ID, CD_WEEKDAYFL)VALUES(?,?)");
				
		ResultSet rs = ps.executeQuery("select distinct D_YEAR from dwdate");
		Map<Integer, String> customerMap = new HashMap<Integer, String>();
		
		Integer a = 0;
		while(rs.next()) {
			a++;
			Integer y = rs.getInt("D_YEAR");
			String aa = Integer.toBinaryString(a);
			while(aa.length()<4)
				aa = "0" + aa;
			
			ps6.execute("INSERT INTO DATE_MAP(D_KEY, D_VALUE, D_TYPE)VALUES('" + y + "', '" + aa +"', 1)");			
			
			ResultSet rs2 = ps2.executeQuery("select distinct D_MONTHNUMINYEAR from dwdate where D_YEAR=" + y);
			
			while(rs2.next()) {			
				Integer m = rs2.getInt("D_MONTHNUMINYEAR");
				String bb = Integer.toBinaryString(m);
				while(bb.length()<4)
					bb = "0" + bb;
				
				ResultSet rs3 = ps3.executeQuery("select distinct D_DAYNUMINMONTH, D_DATEKEY, D_WEEKDAYFL from dwdate where D_YEAR=" + y + " and D_MONTHNUMINYEAR=" + m );
				
				while(rs3.next()) {
					Integer d = rs3.getInt("D_DAYNUMINMONTH");
					Integer id = rs3.getInt("D_DATEKEY");
					//					System.out.println(Integer.toBinaryString(a) + "," + Integer.toBinaryString(b) + "," + Integer.toBinaryString(c));
					String cc = Integer.toBinaryString(d);
					
					while(cc.length()<5)
						cc = "0" + cc;
										
//					System.out.println(aa+bb+cc);
					Long newId = Long.parseLong(aa+bb+cc,2);
					customerMap.put(id, aa+bb+cc);
					System.out.println("id:" + id + ", key:" + aa+bb+cc);
//					ps4.executeUpdate("update DWDATE set ID="+newId+" where ID="+id);
					
					Integer fl = rs3.getInt("D_WEEKDAYFL");
					ps7.setLong(1, newId);
					ps7.setInt(2, rs3.getInt("D_WEEKDAYFL"));
					ps7.addBatch();
				}
			}
		}
		
		ps7.executeBatch();
		con.close();
		return customerMap;
	}
}
