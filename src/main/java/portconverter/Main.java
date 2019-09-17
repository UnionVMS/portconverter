package portconverter;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Main {

	private void go(String[] args, String csv)  {
		

		
		Writer out = null;
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			
			   out = new BufferedWriter(
			            new OutputStreamWriter(new FileOutputStream(
			                    "c:\\HamnarFrånMats\\hamnar.txt"), "UTF-8"));


			conn = DB.open();
			stmt = conn.prepareStatement(
					"select id,geom,nation,county,x,swamcode,port,pt,latitude,longitude,koordinatsystem,version,comment,build from spatial.gbgsmogen order by port asc,pt asc");
			rs = stmt.executeQuery();

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

			int counter = 0;
			
			writeHeader(out);

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
					counter++;

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
					createLine(out, save_port, save_nation, save_county, save_x, save_swamcode, save_pt, latitudesPerPort, longitudesPerPort, save_koordinatsystem, save_version, save_comment, save_build);
					save_port = port;
					latitudesPerPort.clear();
					longitudesPerPort.clear();
				}

				latitudesPerPort.add(latitude);
				longitudesPerPort.add(longitude);
			}
			createLine(out, save_port, save_nation, save_county, save_x, save_swamcode, save_pt, latitudesPerPort, longitudesPerPort, save_koordinatsystem, save_version, save_comment, save_build);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {			
			if(out != null) {
				try {
					out.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			DB.close(stmt);
			DB.close(conn);
		}
	}
	
	

	private void writeHeader(Writer out) {


		
		String line = "\"port\",";
		line += "\"nation\",";
		line += "\"county\",";
		line += "\"x\",";
		line += "\"swamcode\",";
		line += "\"pt\",";
		line += "\"geom\",";
		line += "\"koordinatesystem\",";
		line += "\"version\",";
		line += "\"comment\",";
		line += "\"build\"";
		
		System.out.println(line);
		try {
			out.write(line);
			out.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}



	String formatPortStringAsMultipoint3D(List<String> latitudesPerPort,List<String> longitudesPerPort) {
		String koord = "MULTIPOINT(";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += "(";
			koord += latitudesPerPort.get(i) + ",";
			koord += longitudesPerPort.get(i) + "";
			koord += "),";
		}
		if (koord.endsWith(",")) koord = koord.substring(0, koord.length() - 1);
		koord += ")";
		return koord;
	}
	
	
	//MULTIPOINT(1234.56 6543.21, 1 2, 3 4, 65.21 124.78)
	String formatPortStringAsMultipoint(List<String> latitudesPerPort,List<String> longitudesPerPort) {
		String koord = "MULTIPOINT(";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += latitudesPerPort.get(i) + " ";
			koord += longitudesPerPort.get(i) + "";
			koord += ",";
		}
		if (koord.endsWith(",")) koord = koord.substring(0, koord.length() - 1);
		koord += ")";
		return koord;
	}

	
	
	
	String formatPortStringAsPolygon(List<String> latitudesPerPort,List<String> longitudesPerPort) {
		String koord = "POLYGON(";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += "(";
			koord += latitudesPerPort.get(i) + ",";
			koord += longitudesPerPort.get(i) + "";
			koord += "),";
		}
		if (koord.endsWith(",")) koord = koord.substring(0, koord.length() - 1);
		koord += ")";
		return koord;
	}

	
	
	String createLine(
			Writer out, 
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
			) {
		
		if(save_port.trim().length() < 1) {
			return "";
		}
		
		String line = "";
		
	    line = "\"" + save_port  + "\",";
	    line += "\"" + save_nation  + "\",";
		line += "\"" + save_county + "\",";
		line += "\"" + save_x + "\",";
		line += "\"" + save_swamcode + "\",";
		line += "\"" + save_pt + "\",";

		String koord = formatPortStringAsMultipoint(latitudesPerPort, longitudesPerPort);

		line += "\"" + koord + "\",";

		line += "\"" + save_koordinatsystem + "\",";
		line += "\"" + save_version + "\",";
		line += "\"" + save_comment + "\",";
		line += "\"" + save_build + "\",";
		
		line = line.substring(0, line.length() - 1);
		
		
		System.out.println(line);
		try {
			out.write(line);
			out.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return line;
	}
	

	public static void main(String[] args) {

		String CSV_FILE_PATH = "C:/HamnarFrånMats/hamnar.csv";
		

		Main obj = new Main();
		String uid = "postgres";
		String pwd = "postgres";
		String url = "jdbc:postgresql://localhost:25432/db71u";
		String driver = "org.postgresql.Driver";
		DB.setup(driver, url, uid, pwd);
		obj.go(args, CSV_FILE_PATH);
		DB.shutDown();
	}
}
