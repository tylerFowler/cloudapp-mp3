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

  @Override
  public FileReaderSpout(String inputFilename) {
    super();
    this.inputFilename = inputFilename;
  }

  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    try {
      File file = new File(inputFilename);
      inputReader = new BufferedReader(new FileRader(file));
    } catch (FileNotFoundException e) {
      throw e; // bubble up
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

    String line = inputReader.readLine();

    if (line != null) {
      _collector.emit(new Values(line));
    } else {
      Thread.sleep(10000); // 10 seconds to prevent busy loop
    }
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
    if (inputReader != null) inputReader.close();
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
