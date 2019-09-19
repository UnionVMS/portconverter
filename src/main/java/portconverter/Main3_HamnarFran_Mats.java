package portconverter;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public class Main3_HamnarFran_Mats {

	private final int TO_PORT_AREA = 0;
	private final int TO_PORT = 1;

	private int TARGET_LAYER;

	private void go(String[] args) {

		TARGET_LAYER = TO_PORT_AREA;

		Connection connOutput = null;
		PreparedStatement stmt_output = null;
		Workbook workbook = null;
		try {

			workbook = WorkbookFactory
					.create(new File("C:\\HamnarFrånMats\\2018_05_17 UVMS Gbg o Smögen Hamnar f test.xlsx"));
			System.out.println("Workbook has " + workbook.getNumberOfSheets() + " Sheets : ");

			workbook.forEach(sheet -> {
				System.out.println(sheet.getSheetName());
			});

			Sheet sheet = workbook.getSheetAt(0);
			DataFormatter dataFormatter = new DataFormatter();

			connOutput = DB.openNonPooled();

			switch (TARGET_LAYER) {

			case TO_PORT_AREA:
				stmt_output = connOutput.prepareStatement(
						"insert into spatial.port_area (geom,code,name,enabled) values(ST_GeomFromText(?,4326),?,?,?)");
				break;
			case TO_PORT:
				stmt_output = connOutput.prepareStatement(
						"insert into spatial.port (geom,country_code,code,name,fishing_port,landing_place,commercial_port,enabled) values(ST_GeomFromText(?,4326),?,?,?,?,?,?,?)");
				break;
			}

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

			Iterator<Row> rowIterator = sheet.rowIterator();
			// skip first line
			if (rowIterator.hasNext())
				rowIterator.next();

			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();

				// for (Row row : sheet) {
				String nation = dataFormatter.formatCellValue(row.getCell(0));
				String county = dataFormatter.formatCellValue(row.getCell(1));
				String x = dataFormatter.formatCellValue(row.getCell(2));
				String swamcode = dataFormatter.formatCellValue(row.getCell(3));
				String port = dataFormatter.formatCellValue(row.getCell(4));
				String pt = dataFormatter.formatCellValue(row.getCell(5));
				String latitude = dataFormatter.formatCellValue(row.getCell(6));
				String longitude = dataFormatter.formatCellValue(row.getCell(7));
				String koordinatsystem = dataFormatter.formatCellValue(row.getCell(8));
				String version = dataFormatter.formatCellValue(row.getCell(9));
				String comment = dataFormatter.formatCellValue(row.getCell(10));
				String build = dataFormatter.formatCellValue(row.getCell(11));

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
					createLine(stmt_output, save_port, save_nation, save_county, save_x, save_swamcode, save_pt,
							latitudesPerPort, longitudesPerPort, save_koordinatsystem, save_version, save_comment,
							save_build);
					save_port = port;
					latitudesPerPort.clear();
					longitudesPerPort.clear();
				}

				latitudesPerPort.add(latitude);
				longitudesPerPort.add(longitude);
			}
			createLine(stmt_output, save_port, save_nation, save_county, save_x, save_swamcode, save_pt,
					latitudesPerPort, longitudesPerPort, save_koordinatsystem, save_version, save_comment, save_build);

		} catch (SQLException | EncryptedDocumentException | InvalidFormatException | IOException e) {
			e.printStackTrace();
		} finally {

			if (workbook != null) {
				try {
					workbook.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			DB.close(stmt_output);
			DB.close(connOutput);
		}
	}

	/**
	 * if port table
	 * 
	 * @param latitudesPerPort
	 * @param longitudesPerPort
	 * @return
	 */
	String formatPortStringAsMultipoint(List<String> latitudesPerPort, List<String> longitudesPerPort) {

		String koord = "MULTIPOINT(";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += format(longitudesPerPort.get(i)) + " ";
			koord += format(latitudesPerPort.get(i)) + "";
			koord += ",";
		}
		
		// close the geometry
		koord += format(longitudesPerPort.get(0)) + " ";
		koord += format(latitudesPerPort.get(0));

		
		if (koord.endsWith(","))
			koord = koord.substring(0, koord.length() - 1);
		koord += ")";
		return koord;
	}

	/**
	 * if port_area table
	 * 
	 * @param latitudesPerPort
	 * @param longitudesPerPort
	 * @return
	 */
	String formatPortStringAsMultiPolygon(List<String> latitudesPerPort, List<String> longitudesPerPort) {

		String koord = "MULTIPOLYGON(((";
		for (int i = 0; i < latitudesPerPort.size(); i++) {
			koord += format(longitudesPerPort.get(i)) + " ";
			koord += format(latitudesPerPort.get(i));
			koord += ",";
		}

		// close the geometry
		koord += format(longitudesPerPort.get(0)) + " ";
		koord += format(latitudesPerPort.get(0));

		if (koord.endsWith(","))
			koord = koord.substring(0, koord.length() - 1);
		koord += ")))";
		return koord;
	}

	private String format(String str) {
		return str.replace("°", "");
	}

	void createLine(PreparedStatement stmt, String save_port, String save_nation, String save_county, String save_x,
			String save_swamcode, String save_pt, List<String> latitudesPerPort, List<String> longitudesPerPort,
			String save_koordinatsystem, String save_version, String save_comment, String save_build)
			throws SQLException {

		if (save_port.trim().length() < 1) {
			return;
		}

		String koord = "";
		switch (TARGET_LAYER) {
		case TO_PORT_AREA: {
			koord = formatPortStringAsMultiPolygon(latitudesPerPort, longitudesPerPort);
			stmt.setString(1, koord);
			stmt.setString(2, "SETEST");
			stmt.setString(3, save_port);
			stmt.setBoolean(4, true);
			break;
		}
		case TO_PORT: {
			koord = formatPortStringAsMultipoint(latitudesPerPort, longitudesPerPort);
			// "insert into spatial.port
			// (geom,country_code,code,name,fishing_port,landing_place,commercial_port,enabled,enabled_on)
			// values(?,?,?,?,?,?,?,?,?)");
			stmt.setString(1, koord);
			stmt.setString(2, save_nation);
			stmt.setString(3, save_county);
			stmt.setString(4, save_port);
			stmt.setBoolean(5, true);
			stmt.setBoolean(6, true);
			stmt.setBoolean(7, true);
			stmt.setBoolean(8, true);
			break;
		}
		}

		System.out.print(save_port);
		System.out.print("   ");
		System.out.println(koord);
		// comment this since we don't like accidental additions
		stmt.executeUpdate();

	}

	public static void main(String[] args) {

		Main3_HamnarFran_Mats obj = new Main3_HamnarFran_Mats();
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
