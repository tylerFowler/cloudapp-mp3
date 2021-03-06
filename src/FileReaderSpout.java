import java.io.File;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private BufferedReader inputReader;
  private String inputFilename;

  public FileReaderSpout(String inputFilename) {
    this.inputFilename = inputFilename;
  }

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    try {
      System.out.println("LOADING FILE " + inputFilename);
      File file = new File(this.inputFilename);
      this.inputReader = new BufferedReader(new FileReader(file));
    } catch (Exception e) {
      System.out.println("Got an error " + e);
    }

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

     /*
    ----------------------TODO-----------------------
    Task:
    1. read the next line and emit a tuple for it
    2. don't forget to sleep when the file is entirely read to prevent a busy-loop

    ------------------------------------------------- */
    try {
      String line = inputReader.readLine();

      if (line != null) {
        _collector.emit(new Values(line));
      } else {
        Thread.sleep(10000); // 10 seconds to prevent busy loop
      }
    } catch (Exception e) { System.out.println("Got an error " + e); }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
   /*
    ----------------------TODO-----------------------
    Task: close the file


    ------------------------------------------------- */
    try {
      if (inputReader != null) inputReader.close();
    } catch (Exception e) { System.out.println("Got an error " + e); }
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
