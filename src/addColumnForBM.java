import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class addColumnForBM {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub

		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/ssh", "root", "root123");
		Statement ps = con.createStatement();
		Statement ps2 = con.createStatement();
		char c = 'A';
		char d = 'z';
		char e = 'a';
		int count = 0;
		long begin = 0;
		int record_size = 1000;
		boolean continue_update = true;
		boolean hasRecords = true;
		
		while(true) {
			ResultSet rs = ps2.executeQuery("select LO_ORDERKEY, LO_LINENUMBER from lineorder order by LO_ORDERKEY limit " + begin + "," + record_size);
			
			hasRecords = false;
			while(rs.next()) {
				count++;
				hasRecords = true;
				Integer ln = rs.getInt("LO_LINENUMBER");
				Integer ok = rs.getInt("LO_ORDERKEY");
								
				e = (char) (count%10 + '0');
				
				String s = "";
				
				int t = 0;
				while(t++<20)
					s += e;
				
				System.out.println("insert dic_col:  id:" + count + ", value:" + s);
				int result = ps.executeUpdate("UPDATE lineorder SET BM_COL='"+ s +"' where LO_LINENUMBER=" + ln + " and LO_ORDERKEY=" + ok);
				
	//			if(result <= 0)
	//				continue_update = false;
				System.out.println("insert result:begin:" +begin + ", size:"+record_size +", ok:"+ok+ ",ln:" + ln);
			
			}
			
			if(!hasRecords)
				return;
			
			System.out.println("##########################################");
			begin += record_size;
		}
		
	}

}
