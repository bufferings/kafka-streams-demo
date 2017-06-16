package com.example.demo;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopFive {

  private static final Logger logger = LoggerFactory.getLogger(TopFive.class);

  public static class Item implements Comparable<Item> {
    public final String token;
    public final Integer count;

    public Item(String value) {
      String[] split = value.split(" ");
      token = split[0];
      count = Integer.parseInt(split[1]);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((count == null) ? 0 : count.hashCode());
      result = prime * result + ((token == null) ? 0 : token.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Item other = (Item) obj;
      if (count == null) {
        if (other.count != null)
          return false;
      } else if (!count.equals(other.count))
        return false;
      if (token == null) {
        if (other.token != null)
          return false;
      } else if (!token.equals(other.token))
        return false;
      return true;
    }

    @Override
    public int compareTo(Item o) {
      int result = o.count - this.count;
      if (result != 0) {
        return result;
      }
      return o.token.compareTo(this.token);
    }
  }

  public static TopFive from(GenericRecord record) {
    if (record.getSchema().getName().equals("TweetWordTop5") == false) {
      throw new RuntimeException("Invalid schema");
    }

    TopFive topFive = new TopFive();
    IntStream.rangeClosed(1, 5).forEach(i -> {
      Utf8 value = (Utf8) record.get("a" + Integer.toString(i));
      if (value != null) {
        String itemString = value.toString();
        topFive.add(new Item(itemString));
      }
    });
    return topFive;
  }

  private TreeSet<Item> items = new TreeSet<>();

  private TopFive() {
  }

  public TopFive add(Item item) {
    items.add(item);
    if (items.size() > 5) {
      items.remove(items.last());
    }
    return this;
  }

  public TopFive remove(Item item) {
    items.remove(item);
    return this;
  }

  public GenericRecord to(Schema schema) {
    GenericRecord record = new GenericData.Record(schema);
    int i = 1;
    Iterator<Item> iterator = items.iterator();
    while (iterator.hasNext()) {
      Item item = iterator.next();
      record.put("a" + Integer.toString(i), item.token + " " + item.count);
      i++;
    }
    return record;
  }
}
