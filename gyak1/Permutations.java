import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;

public class Permutations {
  public static void main(String[] args) {
    int n = 4;
    List<Integer> nums = new ArrayList<Integer>();

    populate(nums, n);
    permute(nums);
  }

  private static void populate(List<Integer> numbers, int n) {
    for (int i = 0; i < n; i++) {
      numbers.add(i);
    }
  }

  private static void permute(List<Integer> arr){
    permuteHelper(arr, 0);
  }

  private static void permuteHelper(List<Integer> arr, int k) {
    for (int i = k; i < arr.size(); i++) {
      Collections.swap(arr, i, k);

      permuteHelper(arr, k + 1);

      Collections.swap(arr, k, i);
    }

    if (k == arr.size() - 1) {
      System.out.println(Arrays.toString(arr.toArray()));
    }
  }
}
