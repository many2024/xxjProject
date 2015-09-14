import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class test2 {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection(Configuration.ori_url, Configuration.ori_user, Configuration.ori_pwd);
		Statement ps = con.createStatement();
		Connection con2 = DriverManager.getConnection(Configuration.cmp_url, Configuration.cmp_user, Configuration.cmp_pwd);
		PreparedStatement ps2 = con2.prepareStatement("INSERT INTO RLE_TEST(R_VALUE, LEN) VALUES (?, ?)");
		
		String colName = "LO_ORDERPRIOTITY";
		long begin = 0, record_size = 1000;
		
		Map<String, Integer> dictionary = genDic(con, ps2, colName);
		
		while(true){
			ResultSet rs = ps.executeQuery("select " + colName + " from lineorder limit " + begin + "," + record_size);
			boolean hasRecords = false;
			
			List<String> ops = new ArrayList<String>();
			
			while(rs.next()){
				hasRecords = true;
				
				String op = rs.getString(colName);
				
				ops.add(op);				
			}
			
			
			
			begin += record_size;
			
			if(!hasRecords)
				break;
			else
				rle(ops, ps2, begin, dictionary);
			
		}
		
		ps2.executeBatch();

	}

	private static Map<String, Integer> genDic(Connection con, PreparedStatement ps2, String colName) throws SQLException{
		Statement ps = con.createStatement();
		ResultSet rs = ps.executeQuery("select DISTINCT (BINARY " + colName + "), "+colName+" from lineorder");
		
		Map<String, Integer> dic = new HashMap<String, Integer>();
		
		int index = 1;
		while(rs.next()) {
			dic.put(rs.getString(colName), index);
			index++;
			String sql = "INSERT INTO dictionary(D_KEY, D_VALUE)VALUES('" + rs.getString(colName)+ "', " + index +")";
			System.out.println("insert dictionary sql:" + sql);
			ps2.addBatch(sql);
		}
		
		rs.close();
		return dic;

	}
	
	private static void rle(List<String> ops, PreparedStatement ps2, Long start, Map<String, Integer> dictionary) throws SQLException {
		String s = ops.get(0);
		int len = 1;
		long id = start;
		for(int i=1; i < ops.size(); i++) {
			if(!ops.get(i).equals(s)) {
				System.out.println("rle insert " + ops.get(i) + ", id:" + id + ", length:" + len);
//				ps2.execute("INSERT INTO RLE_TEST(R_VALUE, LENGTH) VALUES (" + dictionary.get(s) + ", " + len + ")");
				ps2.setInt(1, dictionary.get(s));
				ps2.setInt(2, len);
				ps2.addBatch();
				
				s = ops.get(i);
				len = 1;
				id = i + 1;
			}
			else
				len++;
		}
		if(len > 1){
//			ps2.execute("INSERT INTO lineorder(LO_ORDERPRIOTITY, LENGTH) VALUES (" + dictionary.get(s) + ", " + len + ")");
			ps2.setInt(1, dictionary.get(s));
			ps2.setInt(2, len);
			ps2.addBatch();
		}
	}
	
	private static void dictionary(List<String> ops, Map<String, Integer> dic, Statement ps2, Long start) throws SQLException {
		for(int i = 0; i < ops.size(); i++) {
			System.out.println("dictionary: op:"+ops.get(i));
			String sql = "INSERT INTO dic_lineorder(ID, D_VALUE)VALUES(" + (start + i) + ", " + dic.get(ops.get(i)) +")";
			System.out.println("insert into dic_lineorder sql:" + sql);
			ps2.execute(sql);
		}
	}
}
