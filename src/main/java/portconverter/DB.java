package portconverter;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

public class DB {

	private static DataSource dataSource = null;

	/**
	 * 
	 * @param driver
	 * @param connectURI
	 * @param uid
	 * @param pwd
	 */
	public static void setup(String driver, String connectURI, String uid, String pwd) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(driver);
		ds.setUsername(uid);
		ds.setPassword(pwd);
		ds.setUrl(connectURI);
		ds.setDefaultAutoCommit(true);
		dataSource = (DataSource) ds;
	}

	private static String nonPooledDriver;
	private static String nonPooledconnectURI;
	private static String nonPooledUid;
	private static String nonPooledPwd;

	public static void setupNonPooled(String driver, String connectURI, String uid, String pwd) {
		nonPooledDriver = driver;
		nonPooledconnectURI = connectURI;
		nonPooledUid = uid;
		nonPooledPwd = pwd;
	}

	/**
	 * 
	 */
	public static void shutDown() {
		if (dataSource != null) {
			BasicDataSource bds = (BasicDataSource) dataSource;
			try {
				bds.close();
				dataSource = null;
			} catch (SQLException e) {
			}
		}

	}

	/**
	 * 
	 * @return
	 */
	public static boolean isStarted() {
		return dataSource != null;
	}

	/**
	 * 
	 */
	public static void printDataSourceStats() {
		if (dataSource != null) {
			BasicDataSource bds = (BasicDataSource) dataSource;
		}
	}

	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection open() throws SQLException {
		return dataSource.getConnection();
	}

	public static Connection openNonPooled() throws SQLException {
		Connection connection = DriverManager.getConnection(nonPooledconnectURI, nonPooledUid, nonPooledPwd);
		return connection;
		// Class.forName("org.postgresql.Driver");
	}

	/**
	 * 
	 * @param conn
	 */
	public static void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * 
	 * @param stmt
	 */
	public static void close(PreparedStatement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * 
	 * @param stmt
	 */
	public static void close(CallableStatement stmt) {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
			}
		}
	}

	/**
	 * 
	 * @param rs
	 */
	public static void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
			}
		}
	}

}