class Solution:
    def __init__(self):
        self.memo = {}

    def climbStairs(self, n:int) -> int:
        if n==1:
            return 1
        elif n==2:
            return 2

        if n in self.memo:
            return self.memo[n]

        self.memo[n] = self.climbStairs(n-1) + self.climbStairs(n-2)

        return self.memo[n]

sol = Solution()
print(sol.climbStairs(5))