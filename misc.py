from termcolor import colored

def print_log(type, name, message, tabs=0):
  """A canonical way to print messages to the console, printing the function that called it with a meaningful color."""
  if type == 'ok':
    col = 'green'
    attrs = None
  elif type == 'error':
    col = 'red'
    attrs = None
  elif type == 'warning':
    col = 'yellow'
    attrs = None
  else:
    col = attrs = None
  print '==>'*tabs + colored(name, col, attrs) + ': ' + message
  
def percentagize_list(list):
  sum = 0
  for x in list: sum += x[1]
  return [ [x[0], 100.0*x[1]/sum] for x in list]