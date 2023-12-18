package ratelimiter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RateLimiter {

  private final Jedis redis;
  private final String label;
  private final long maxRequestCount;
  private final long timeWindowSeconds;

  public RateLimiter(Jedis redis, String label, long maxRequestCount, long timeWindowSeconds) {
    this.redis = redis;
    this.label = label;
    this.maxRequestCount = maxRequestCount;
    this.timeWindowSeconds = timeWindowSeconds;
  }

  /*
   * Для определения количества запросов воспользуемся отсортированным множеством. Множеству укажем ключ по полю label
   * класса RateLimiter. В качестве score укажем время запроса от начала эпохи Instant в мс, в качестве
   * member - количество всех запросов в виде String. Количество элементов в этом множестве считаем по score в пределах
   * от начала временного окна до текущего момента. Количество таких элементов соответствует количеству запросов
   * в текущем временном окне.
   * */
  public boolean pass() {
    String key = label;
    long current_time = Instant.now().toEpochMilli();
    long window_begin = current_time - timeWindowSeconds * 1000;
    redis.zremrangeByScore(key, 0, window_begin - 1);
    long request_count = redis.zcard(key);
    if (request_count < maxRequestCount){
      redis.zadd(key, current_time, Long.toString(current_time));
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    JedisPool pool = new JedisPool("localhost", 6379);

    try (Jedis redis = pool.getResource()) {
      RateLimiter rateLimiter = new RateLimiter(redis, "pr_rate", 1, 1);

      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      long prev = Instant.now().toEpochMilli();
      long now;

      while (true) {
        try {
          String s = br.readLine();
          if (s == null || s.equals("q")) {
            return;
          }
          boolean passed = rateLimiter.pass();

          now = Instant.now().toEpochMilli();
          if (passed) {
            System.out.printf("%d ms: %s", now - prev, "passed");
            prev = now;
          } else {
            System.out.printf("%d ms: %s", now - prev, "limited");
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }
  }
}
