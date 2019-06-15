# Given an list of at least 3 points, calculate whether x is within the bounds of those points

# Variables
x = [0, 0, 5, 5]
y = [0, 5, 5, 0]

class sq_extent():
    right_x = 0
    left_x = 0
    up_y = 0
    down_y = 0

class point():
    x = 2
    y = 2

for num in x:
    if num > sq_extent.right_x:
        sq_extent.right_x = num
    if num < sq_extent.left_x:
        sq_extent.left_x = num
    else:
        continue

for num in y:
    if num > sq_extent.up_y:
        sq_extent.up_y = num
    if num < sq_extent.down_y:
        sq_extent.down_y = num
    else:
        continue

print('Upper extent:', sq_extent.up_y)
print('Lower extent:', sq_extent.down_y)
print('Right extent:', sq_extent.right_x)
print('Left extent:', sq_extent.left_x)

if point.x > sq_extent.left_x and point.x < sq_extent.right_x and point.y > sq_extent.down_y and point.y < sq_extent.up_y:
    print('Within square bounds')
else:
    print('Not within square bounds')