package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.JTableHeader;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.*;

import static com.sun.glass.ui.Cursor.setVisible;
import static org.apache.spark.sql.functions.col;

public class Erasmus extends JFrame {
    private JLabel Table;
    private JComboBox tabel;
    private JScrollPane Panel;


    public Erasmus() {
        setContentPane(Table);
        setTitle("Countries");
        setSize(550, 500);
        setLocation(700, 250);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setVisible(true);
    }



    public static void main(String[] args) {
        Erasmus er = new Erasmus();
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("C:/Users/40759/IdeaProjects/untitled10/src/main/resources/erasmus.csv");


    }
}


