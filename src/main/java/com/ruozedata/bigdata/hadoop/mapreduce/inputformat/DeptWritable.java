package com.ruozedata.bigdata.hadoop.mapreduce.inputformat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeptWritable implements DBWritable, Writable {

    private int deptno;
    private String dname;
    private String loc;

    public DeptWritable() {
    }

    public DeptWritable(int deptno, String dname, String loc) {
        this.deptno = deptno;
        this.dname = dname;
        this.loc = loc;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

    public String getName() {
        return dname;
    }

    public void setName(String dname) {
        this.dname = dname;
    }

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
        this.loc = loc;
    }

    @Override
    public String toString() {
        return deptno + "\t" + dname + "\t" + loc;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(deptno);
        out.writeUTF(dname);
        out.writeUTF(loc);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.deptno = in.readInt();
        this.dname = in.readUTF();
        this.loc = in.readUTF();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,deptno);
        statement.setString(2,dname);
        statement.setString(3,loc);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        deptno = resultSet.getInt(1);
        dname = resultSet.getString(2);
        loc = resultSet.getString(3);
    }
}
