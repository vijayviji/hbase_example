package org.example;

import com.google.common.base.Throwables;

public class LogObj {
  private final StringBuilder builder = new StringBuilder();

  public LogObj(String eventName) {
    addSkippingColon("event", eventName);
  }

  public LogObj add(final Throwable throwable) {
    return add("throwableName", throwable.getClass().getSimpleName())
        .add("throwableMessage", throwable.getMessage())
        .add("throwableStack", Throwables.getStackTraceAsString(throwable));
  }

  public LogObj add(String key, Object val) {
    add(key, val, false, true);
    return this;
  }

  public LogObj addWithQuotes(String key, Object val) {
    add(key, val, true, true);
    return this;
  }

  public String toString() {
    return builder.toString();
  }

  private LogObj addSkippingColon(String key, Object val) {
    add(key, val, false, false);
    return this;
  }

  private LogObj add(String key, Object val, boolean quotes, boolean colonPrefix) {
    if (colonPrefix) {
      builder.append(";");
    }

    builder.append(key).append("=");
    if (quotes) {
      builder.append("\"");
    }

    builder.append(val);

    if (quotes) {
      builder.append("\"");
    }

    return this;
  }
}
