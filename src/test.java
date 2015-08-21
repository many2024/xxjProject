import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.PseudoColumnUsage;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class test {
	private static String hexStr =  "0123456789ABCDEF";

	private static String[] binaryArray =  
        {"0000","0001","0010","0011",  
        "0100","0101","0110","0111",  
        "1000","1001","1010","1011",  
        "1100","1101","1110","1111"}; 
	
	
	
	private static String colName = "LO_SHIPMODE";
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("com.mysql.jdbc.Driver");
		Map<Integer, String> dateMap = prepareDate();
		Map<Integer, String> customerMap = prepareCustomer();
		
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/ssh", "root", "root123");
		Statement ps = con.createStatement();
		Connection con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/compressed_ssh", "root", "root123");
		Statement ps2 = con2.createStatement();
		Long begin = 0L;
		int record_size = 1000;
		Map<String, Integer> dic = genDic(con, ps2, colName);

		while(true) {
		ResultSet rs = ps.executeQuery("select LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_ORDERDATE, " + colName + " from lineorder order by LO_ORDERKEY limit " + begin + "," + record_size);
		boolean hasRecords = false;
		
		long count = begin;
		int count2 = 1;
		int aveLen = 0;
		int totalLen = 0;
		String ss = "";
		int ii = 0;
		int jj = 0;
		List<String> ops = new ArrayList<String>();
		Set<String> opSet = new HashSet<String>();
		while(rs.next()) {
			hasRecords = true;
			count ++;
			Integer c = rs.getInt("LO_CUSTKEY");
			Integer d = rs.getInt("LO_ORDERDATE");
			Integer ln = rs.getInt("LO_LINENUMBER");
			Integer ok = rs.getInt("LO_ORDERKEY");
			String op = rs.getString(colName);
			
			String lns = Integer.toBinaryString(ln);
			if(lns.length() < 4)
				lns = "0" + lns;
			
			if(!op.equals(ss)) {
				if(jj != 0) {
					ii += jj + 1;
					totalLen += jj + 1;
					aveLen = totalLen/count2;
					
					count2++;
				}
				jj = 0;
				ss = op;
			}
			else {
				jj++;
			}
			
//			System.out.println("insert string id:" + dateMap.get(d)+customerMap.get(c) + lns);
			
			Long id = Long.parseLong(dateMap.get(d)+customerMap.get(c) + lns, 2);
			
			ops.add(op);
			opSet.add(op);
//			ps2.execute("INSERT INTO lineorder(ID, LO_ORDERPRIOTITY) VALUES (" + count + ", '" + rs.getString("LO_ORDERPRIOTITY") + "')");
			ps2.execute("INSERT INTO ID_MAP(ID, LO_KEY, a, b) VALUES (" + count + ", '" + id + "', " + ok + ", " + ln + ")");
			System.out.println("#" + count +": insert id:" + id);
			
			
		}
		
		System.out.println("begin:" + begin + ", size:" + ops.size());
		if(jj != 0)
			ii += jj;
		
		
		double sr = (ii*1.0*100)/1000;
		System.out.println("sequence rate:" + sr + "%, average length:" + aveLen);
		
		if(sr > 20.0 && aveLen > 4) {
			rle(ops, ps2, begin);
		}
		else {
			if(opSet.size() < 4) {
				bitMap(opSet, ps2, ops, begin);
			}
			else {
				dictionary(ops, dic, ps2, begin);
			}
		}
		
		if(!hasRecords)
			return;
		begin += record_size;
		
		}
	}
	
	private static void rle(List<String> ops, Statement ps2, Long start) throws SQLException {
		String s = ops.get(0);
		int len = 1;
		long id = start;
		for(int i=1; i < ops.size(); i++) {
			if(!ops.get(i).equals(s)) {
				System.out.println("rle insert " + ops.get(i) + ", id:" + id + ", length:" + len);
				ps2.execute("INSERT INTO lineorder(ID, LO_ORDERPRIOTITY, LENGTH) VALUES (" + id + ", '" + s + "', " + len + ")");
				
				s = ops.get(i);
				len = 1;
				id = i + 1;
			}
			else
				len++;
		}
		if(len > 1)
			ps2.execute("INSERT INTO lineorder(ID, LO_ORDERPRIOTITY, LENGTH) VALUES (" + id + ", '" + s + "', " + len + ")");
	}
	
	private static Map<String, Integer> genDic(Connection con, Statement ps2, String colName) throws SQLException{
		Statement ps = con.createStatement();
		ResultSet rs = ps.executeQuery("select DISTINCT (BINARY " + colName + "), "+colName+" from lineorder");
		
		Map<String, Integer> dic = new HashMap<String, Integer>();
		
		int index = 1;
		while(rs.next()) {
			dic.put(rs.getString(colName), index);
			index++;
			String sql = "INSERT INTO dictionary(D_KEY, D_VALUE)VALUES('" + rs.getString(colName)+ "', " + index +")";
			System.out.println("insert dictionary sql:" + sql);
			ps2.execute(sql);
		}
		
		rs.close();
		return dic;

	}
	
	private static void dictionary(List<String> ops, Map<String, Integer> dic, Statement ps2, Long start) throws SQLException {
		for(int i = 0; i < ops.size(); i++) {
			System.out.println("dictionary: op:"+ops.get(i));
			String sql = "INSERT INTO dic_lineorder(ID, D_VALUE)VALUES(" + (start + i) + ", " + dic.get(ops.get(i)) +")";
			System.out.println("insert into dic_lineorder sql:" + sql);
			ps2.execute(sql);
		}
	}
	
	private static void bitMap(Set<String> opsSet, Statement ps2, List<String> ops, Long start) throws SQLException {
		if(opsSet != null && !opsSet.isEmpty()) {
			for(String op: opsSet) {
				String s = "";
				for(String p: ops) {
					if(p.equals(op))
						s += "1";
					else
						s += "0";
				}
				
				System.out.println("bit map insert: key:" + s + ", value:" + op);
				ps2.execute("INSERT INTO bit_map(ID, B_KEY, B_VALUE)VALUES(" + start + ", '" + s + "', '" + op + "')");
			}
		}
	}
	
	private static Map<Integer, String> prepareCustomer() throws SQLException{
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/ssh", "root", "root123");
		Statement ps = con.createStatement();
		Statement ps5 = con.createStatement();
		Statement ps2 = con.createStatement();
		Statement ps3 = con.createStatement();
		Statement ps4 = con.createStatement();
//		Connection con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/compressed_ssh", "root", "root123");
//		Statement ps6 = con2.createStatement();
				
		ResultSet rs = ps.executeQuery("select distinct C_REGION from customer order by C_REGION");
		Map<Integer, String> dateMap = new HashMap<Integer, String>();
		
		Integer a = 0;
		while(rs.next()) {
			a++;
			String y = rs.getString("C_REGION");
			
			ResultSet rs2 = ps2.executeQuery("select distinct C_NATION from customer where C_REGION='" + y + "' order by C_REGION");
			
			Integer b = 0;
			while(rs2.next()) {
				b++;
				String m = rs2.getString("C_NATION");
				ResultSet rs3 = ps3.executeQuery("select distinct C_CITY from customer where C_REGION='" + y + "' and C_NATION='" + m + "' order by C_CITY");
				
				Integer c = 0;
				while(rs3.next()) {
					c++;
					String d = rs3.getString("C_CITY");
					ResultSet rs4 = ps4.executeQuery("select distinct C_NAME, C_CUSTKEY, C_MKTSEGMENT from customer where C_REGION='" + y + "' and C_NATION='" + m + "' and C_CITY='" + d + "' order by C_NAME");
					
					Integer e = 0;
					while(rs4.next()) {
						e++;
						String name = rs4.getString("C_NAME");
						Integer id = rs4.getInt("C_CUSTKEY");
						String mktSegment = rs4.getString("C_MKTSEGMENT");
//						System.out.println(Integer.toBinaryString(a) + "," + Integer.toBinaryString(b) + "," + Integer.toBinaryString(c) + ", " + Integer.toBinaryString(e));
						String aa = Integer.toBinaryString(a);
						String bb = Integer.toBinaryString(b);
						String cc = Integer.toBinaryString(c);
						String ee = Integer.toBinaryString(e);
						
						while(aa.length()<3)
							aa = "0" + aa;
						while(bb.length()<6)
							bb = "0" + bb;
						while(cc.length()<6)
							cc = "0" + cc;
						while(ee.length() < 10)
							ee = "0" + ee;
						
//						System.out.println(aa+bb+cc+ee);
						Long newId = Long.parseLong(aa+bb+cc+ee,2);
						dateMap.put(id, aa+bb+cc+ee);
	//					ps4.executeUpdate("update DWDATE set ID="+newId+" where ID="+id);
//						ps6.execute("INSERT INTO customer()")
					}
				}
			}
		}
		
		con.close();
		
		return dateMap;
	}
	
	private static Map<Integer, String> prepareDate() throws SQLException {
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/ssh", "root", "root123");
		Statement ps = con.createStatement();
		Statement ps5 = con.createStatement();
		Statement ps2 = con.createStatement();
		Statement ps3 = con.createStatement();
		Statement ps4 = con.createStatement();
//		Connection con2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/compressed_ssh", "root", "root123");
//		Statement ps6 = con2.createStatement();
				
		ResultSet rs = ps.executeQuery("select distinct D_YEAR from dwdate order by D_YEAR");
		Map<Integer, String> customerMap = new HashMap<Integer, String>();
		
		Integer a = 0;
		while(rs.next()) {
			a++;
			Integer y = rs.getInt("D_YEAR");
			
			ResultSet rs2 = ps2.executeQuery("select distinct D_MONTH from dwdate where D_YEAR=" + y + " order by D_MONTH");
			
			Integer b = 0;
			while(rs2.next()) {
				b++;
				String m = rs2.getString("D_MONTH");
				ResultSet rs3 = ps3.executeQuery("select distinct D_DATE, D_DATEKEY from dwdate where D_YEAR=" + y + " and d_MONTH='" + m + "' order by D_DATE");
				
				Integer c = 0;
				while(rs3.next()) {
					c++;
					String d = rs3.getString("D_DATE");
					Integer id = rs3.getInt("D_DATEKEY");
//					System.out.println(Integer.toBinaryString(a) + "," + Integer.toBinaryString(b) + "," + Integer.toBinaryString(c));
					String aa = Integer.toBinaryString(a);
					String bb = Integer.toBinaryString(b);
					String cc = Integer.toBinaryString(c);
					
					while(aa.length()<4)
						aa = "0" + aa;
					while(bb.length()<4)
						bb = "0" + bb;
					while(cc.length()<5)
						cc = "0" + cc;
					
//					System.out.println(aa+bb+cc);
					Long newId = Long.parseLong(aa+bb+cc,2);
					customerMap.put(id, aa+bb+cc);
//					ps4.executeUpdate("update DWDATE set ID="+newId+" where ID="+id);
				}
			}
		}
		con.close();
		return customerMap;
	}	
	
	public static String BinaryToHexString(byte[] bytes){  
        
        String result = "";  
        String hex = "";  
        for(int i=0;i<bytes.length;i++){  
            //字节高4位  
            hex = String.valueOf(hexStr.charAt((bytes[i]&0xF0)>>4));  
            //字节低4位  
            hex += String.valueOf(hexStr.charAt(bytes[i]&0x0F));  
            result +=hex+" ";  
        }
        return result;  
	}
}
