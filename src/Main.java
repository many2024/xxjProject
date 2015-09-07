import java.sql.SQLException;
import java.util.Map;


public class Main {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub

		PreDate preDate = new PreDate();		
		Map<Integer, String> dateMap = preDate.pre();
		
		PreCustomer preCustomer = new PreCustomer();
		Map<Integer, String> customerMap = preCustomer.pre();
		
		PreLineorder preLineorder = new PreLineorder();
		preLineorder.pre(dateMap, customerMap);
	}

}
