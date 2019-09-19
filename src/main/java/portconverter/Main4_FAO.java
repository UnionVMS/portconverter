package portconverter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Main4_FAO {

	private void go(String[] args) {

		Connection connInput = null;
		Connection connOutput = null;
		PreparedStatement stmt_input = null;
		PreparedStatement stmt_output = null;
		ResultSet rs = null;
		try {

			connInput = DB.open();

			stmt_input = connInput.prepareStatement(
					"select ocean, subocean, f_area,f_subarea,f_division,f_subdivis,f_subunit,id,geom,f_code,name_en from spatial.fao_areas");

			connOutput = DB.openNonPooled();

			stmt_output = connOutput.prepareStatement(
					"insert into spatial.fao (ocean,subocean,f_area,f_subarea,f_division,f_subdivis,f_subunit,gid, enabled,geom,code,f_label,name) values(?,?,?,?,?,?,?,?,?,?,?,?,?)");

			rs = stmt_input.executeQuery();

			while (rs.next()) {

				String ocean = rs.getString(1);
				String subocean = rs.getString(2);
				String f_area = rs.getString(3);
				String f_subarea = rs.getString(4);
				String f_division = rs.getString(5);
				String f_subdivis = rs.getString(6);
				String f_subunit = rs.getString(7);
				Integer id = rs.getInt(8);
				Object geom = rs.getObject(9);
				String code = rs.getString(10);
				String name = rs.getString(11);
				if (name != null) {
					if (name.length() > 50) {
						name = name.substring(0, 50);
					}
				}

				System.out.print(ocean + ", ");
				System.out.print(subocean + ", ");
				System.out.print(f_area + ", ");
				System.out.print(f_subarea + ", ");
				System.out.print(f_division + ", ");
				System.out.print(f_subdivis + ", ");
				System.out.print(f_subunit + ", ");
				System.out.print(id + ", ");
				System.out.println(geom + ", ");
				System.out.print(code + ", ");
				System.out.println(name);

				stmt_output.setString(1, ocean);
				stmt_output.setString(2, subocean);
				stmt_output.setString(3, f_area);
				stmt_output.setString(4, f_subarea);
				stmt_output.setString(5, f_division);
				stmt_output.setString(6, f_subdivis);
				stmt_output.setString(7, f_subunit);
				stmt_output.setInt(8, id);
				stmt_output.setBoolean(9, true);
				stmt_output.setObject(10, geom);
				stmt_output.setString(11, code);
				stmt_output.setString(12, code);
				stmt_output.setString(13, name);

				stmt_output.executeUpdate();

			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {

			DB.close(rs);
			DB.close(stmt_input);
			DB.close(stmt_output);
			DB.close(connOutput);
			DB.close(connInput);
		}
	}

	public static void main(String[] args) {

		Main4_FAO obj = new Main4_FAO();
		String uid = "postgres";
		String pwd = "postgres";
		String url = "jdbc:postgresql://localhost:25432/db71u";
		String driver = "org.postgresql.Driver";
		DB.setup(driver, url, uid, pwd);
		DB.setupNonPooled(driver, "jdbc:postgresql://livmdb71p:5432/db71p", uid, pwd);
		// DB.setupNonPooled(driver, "jdbc:postgresql://localhost:25432/db71u", uid,
		// pwd);
		obj.go(args);
		DB.shutDown();
	}
}
