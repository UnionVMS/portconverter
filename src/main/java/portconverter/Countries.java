package portconverter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Countries {

	private String rm(String inp) {
		if (inp == null)
			return "";
		inp = inp.replace(",", "");
		return inp;
	}

	private void go(String[] args) {

		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		FileOutputStream out = null;

		try {

			File f = new File("c:\\temp\\countries.csv");
			if (!f.exists()) {
				f.createNewFile();
			} else {
				f.delete();
				f.createNewFile();
			}

			List<String> dup2control = new ArrayList<>();
			List<String> dup3control = new ArrayList<>();

			out = new FileOutputStream(f, false);

			out.write("code2,code3,name\n".getBytes("utf-8"));

			conn = DB.open();
			stmt = conn.prepareStatement("select cntr_id,code,name from spatial.countries");
			rs = stmt.executeQuery();
			while (rs.next()) {
				String id = rs.getString(1);
				String code = rs.getString(2);
				String name = rs.getString(3);
				String line = rm(id) + ",";
				line += rm(code) + ",";
				line += rm(name);
				line += "\n";

				out.write(line.getBytes("utf-8"));

				String w = "";
				if (code.length() == 2) {
					w = "     -----code is 2 in length " + name;
				}
				if (code.equals(id)) {
					w += "     -----code and id are equal ";
				}
				if (dup2control.contains(id)) {
					w += " code 2 duplicate";
				} else {
					dup2control.add(id);
				}
				if (dup3control.contains(code)) {
					w += " code 3 duplicate";
				} else {
					dup3control.add(code);
				}

				if (w.length() > 0) {
					System.out.print(w);
					System.out.println();
				}
			}
		} catch (SQLException | IOException e) {

			System.out.println(e.toString());

		} finally {

			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			DB.close(rs);
			DB.close(stmt);
			DB.close(conn);

		}

	}

	public static void main(String[] args) {

		Countries obj = new Countries();
		String uid = "postgres";
		String pwd = "postgres";
		String url = "jdbc:postgresql://localhost:25432/db71u";
		String driver = "org.postgresql.Driver";
		DB.setup(driver, url, uid, pwd);
		// DB.setupNonPooled(driver, "jdbc:postgresql://livmdb71u:5432/unionvmsdev",
		// uid, pwd); // dev u
		// DB.setupNonPooled(driver, "jdbc:postgresql://livmdb71t:5432/db71t", uid,
		// pwd); // test
		// DB.setupNonPooled(driver, "jdbc:postgresql://livmdb71p:5432/db71p", uid,
		// pwd); // prod
		DB.setupNonPooled(driver, "jdbc:postgresql://localhost:25432/db71u", uid, pwd);
		obj.go(args);
		DB.shutDown();
	}

}
