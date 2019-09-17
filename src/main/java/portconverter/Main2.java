package portconverter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main2 {

	private void go(String[] args)  {
		

		
		Connection connInput = null;
		Connection connOutput = null;
		PreparedStatement stmt_input = null;
		PreparedStatement stmt_output = null;
		ResultSet rs = null;
		try {

			connInput = DB.open();
			
			stmt_input = connInput.prepareStatement(
					"select id,geom,nation,county,x,swamcode,port,pt,latitude,longitude,koordinatsystem,version,comment,build from spatial.gbgsmogen order by port asc,pt asc");
						
			//stmt_output = conn.prepareStatement(
			//		"insert into spatial.port (geom,country_code,code,name,fishing_port,landing_place,commercial_port,enabled) values(ST_GeomFromText(?,4326),?,?,?,?,?,?,?)");

			connOutput = DB.openNonPooled();

			stmt_output = connOutput.prepareStatement(
					"insert into spatial.port_area (geom,code,name,enabled) values(ST_GeomFromText(?,4326),?,?,?)");
			
			rs = stmt_input.executeQuery();

			List<String> latitudesPerPort = new ArrayList<>();
			List<String> longitudesPerPort = new ArrayList<>();

			String save_nation = "";
			String save_county = "";
			String save_x = "";
			String save_swamcode = "";
			String save_port = "";
			String save_pt = "";
			String save_latitude = "";
			String save_longitude = "";
			String save_koordinatsystem = "";
			String save_version = "";
			String save_comment = "";
			String save_build = "";

			while (rs.next()) {

				String nation = rs.getString(3);
				String county = rs.getString(4);
				String x = rs.getString(5);
				String swamcode = rs.getString(6);
				String port = rs.getString(7);
				String pt = rs.getString(8);
				String latitude = rs.getString(9);
				String longitude = rs.getString(10);
				String koordinatsystem = rs.getString(11);
				String version = rs.getString(12);
				String comment = rs.getString(13);
				String build = rs.getString(14);

				// port break (new port)
				if (!port.equals(save_port)) {
					save_nation = nation;
					save_county = county;
					save_x = x;
					save_swamcode = swamcode;
					save_pt = pt;
					save_latitude = latitude;
					save_longitude = longitude;
					save_koordinatsystem = koordinatsystem;
					save_version = version;
					save_comment = comment;
					save_build = build;				
					createLine(stmt_output,save_port, save_nation, save_county, save_x, save_swamcode, save_pt, latitudesPerPort, longitudesPerPort, save_koordinatsystem, save_version, save_comment, save_build);
					save_port = port;
					latitudesPerPort.clear();
					longitudesPerPort.clear();
				}

				latitudesPerPort.add(latitude);
				longitudesPerPort.add(longitude);
			}
			createLine(stmt_output, save_port, save_nation, save_county, save_x, save_swamcode, save_pt, latitudesPerPort, longitudesPerPort, save_koordinatsystem, save_version, save_comment, save_build);
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
	
	
	
	String formatPortStringAsMultipoint(List<String> latitudesPerPort,List<String> longitudesPerPort) {
		
		String koord = "MULTIPOINT(";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += longitudesPerPort.get(i) + " ";
			koord += latitudesPerPort.get(i) + "";
			koord += ",";
		}
		if (koord.endsWith(",")) koord = koord.substring(0, koord.length() - 1);
		koord += ")";
		return koord;
	}
	
	String formatPortStringAsMultiPolygon(List<String> latitudesPerPort,List<String> longitudesPerPort) {
		
		String koord = "MULTIPOLYGON(((";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += longitudesPerPort.get(i) + " ";
			koord += latitudesPerPort.get(i) ;
			koord += ",";
		}
		
		// close the geometry
		koord += longitudesPerPort.get(0) + " ";
		koord += latitudesPerPort.get(0) ;
		
		if (koord.endsWith(",")) koord = koord.substring(0, koord.length() - 1);
		koord += ")))";
		return koord;
	}


	
	void createLine(
			PreparedStatement stmt,
			String save_port,
			String save_nation,
			String save_county,
			String save_x,
			String save_swamcode,
			String save_pt,
			List<String> latitudesPerPort,
			List<String> longitudesPerPort,
			
			String save_koordinatsystem,
			String save_version,
			String save_comment,
			String save_build
			) throws SQLException {
		
		if(save_port.trim().length() < 1) {
			return ;
		}
		
		

		String koord = formatPortStringAsMultiPolygon(latitudesPerPort, longitudesPerPort);

//	"select id,geom,nation,county,x,swamcode,port,pt,latitude,longitude,koordinatsystem,version,comment,build from spatial.gbgsmogen order by port asc,pt asc");		
//	"insert into spatial.port (geom,country_code,code,name,fishing_port,landing_place,commercial_port,enabled,enabled_on) values(?,?,?,?,?,?,?,?,?)");
//	"insert into spatial.port_area (geom,code,name,enabled) values(ST_GeomFromText(?,4326),?,?,?)");
		
		stmt.setString(1, koord);
		stmt.setString(2, "SETEST");
		stmt.setString(3, save_port);
		stmt.setBoolean(4, true);
		
		stmt.executeUpdate();
		
		
	}
	

	public static void main(String[] args) {

		Main2 obj = new Main2();
		String uid = "postgres";
		String pwd = "postgres";
		String url = "jdbc:postgresql://localhost:25432/db71u";
		String driver = "org.postgresql.Driver";
		DB.setup(driver, url, uid, pwd);
		DB.setupNonPooled(driver, "jdbc:postgresql://livmdb71p:5432/db71p", uid, pwd);		
		obj.go(args);
		DB.shutDown();
	}
}
