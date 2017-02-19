public class Hello {
  public static void main(String[] args) {
    System.out.println(isLoopYear(2010));
    System.out.println(isLoopYear(2012));
    System.out.println(isLoopYear(2100));
  }

  private static boolean isLoopYear(int year) {
    return (year % 4 == 0) && ((year % 100 == 0 && year % 400 == 0) || (year % 100 != 0 && year % 400 != 0));
  }
}
