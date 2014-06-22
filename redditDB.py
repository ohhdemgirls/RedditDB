import sys, time, re, string              # Core Python libraries
import praw                               # 3rd party libraries
import mongo_class                        # My libraries
from termcolor import colored             # Individual imports
from misc import print_log

# A "post" is defined to be either a comment or a submission.

def load_common_words(filename):
  common_words = []  
  f = open(filename, 'r')
  common_words = f.read().split()
  f.close()
  return common_words#.append('')

class redditDB:
  """A class to interface with and maintain a database of Reddit posts, stored with MongoDB."""
  def __init__(self, name):
    self.name = name
    self.r = None
    self.m = None
  
  def connect_to_reddit(self, user_agent):
    """Connects this class to Reddit.  Automatically called if needed."""
    try:
      self.r = praw.Reddit(user_agent)
      print_log('ok', self.name + '.connect_to_reddit', 'Connected with user_agent \'' + user_agent +'\'')
    except Exception as e:
      print_log('error', self.name + '.connect_to_reddit', str(e))
  
  def connect_to_database(self, UID, db):
    """Connects this class to the Mongo database.  Must be called before anything else may occur."""
    try:
      self.m = mongo_class.mongoClass(self.name + '.mongoDB')
      self.m.open(UID)
      self.m.set_db(db)
      #print_log('ok', self.name + '.connect_to_database', 'Connected to  ' + UID +'.')
    except Exception as e:
      print_log('error', self.name + '.connect_to_database', str(e))
    
  def disconnect_from_database(self):
    """Ensures there are no open Mongo connections."""
    self.m.close()
    
  def add_submission(self, reddit_id, tabs = 0, verbose = False):
    """Download a Reddit submission by reddit_id, and add it to the database.  Checks for existence first."""
    mongo = self.m.db.posts.find_one({'id': reddit_id})
    if mongo == None:
      try:
        post = self.r.get_submission(submission_id = reddit_id)
      except Exception as e:
        print_log('error', self.name + '.add_submission', str(e)+', Post ID '+reddit_id)
        return
      data = {  'id': post.id,
                'type': 'submission',
                'author': str(post.author).lower(),
                'created_utc': post.created_utc,
                'title': post.title,
                'subreddit': '%s'%post.subreddit,
                'domain': post.domain,
                'selftext': post.selftext,
                'ups': post.ups,
                'downs': post.downs,
                'gilded': post.gilded,
                'num_comments': post.num_comments
              }
      mongo_id = self.m.db.posts.insert(data)
      if verbose: print_log('ok', self.name+'.add_submission', 'Added post ID ' + reddit_id + ' to the database.', tabs=tabs)
      return [mongo_id, 1]
    else:
      if verbose: print_log('warning', self.name+'.add_submission', 'Post ID ' + reddit_id + ' already exists in the database.', tabs=tabs)
      return [mongo['_id'], 0]
  
  def add_redditor(self, username, verbose = False):
    """Downloads all reddit posts by a user, and adds them to the database.  Checks for existence first."""
    start_time = time.time()
    total_submissions_added = 0
    total_comments_added = 0
    total_submissions_skipped = 0
    total_comments_skipped = 0
    total_bonus_submissions = 0
    comment_submission_ids = []
    try:
      u = self.r.get_redditor(username)
      submissions = u.get_submitted(limit=None)
    except Exception as e:
      print_log('error', self.name + '.add_redditor.get_submitted', str(e))
      return None
    print colored(self.name + '.add_Redditor:', 'green')
    for post in submissions:                # Get submissions
      if not verbose:
        elapsed_time = time.time()-start_time
        m, s = divmod(elapsed_time, 60)
        h, m = divmod(m, 60)
        elapsed_time_string = '%d:%02d:%02d' % (h, m, s)        
        sys.stdout.flush()
        sys.stdout.write('\r'+' '*80)
        sys.stdout.write('\r\t' + colored('Added', 'green') + '/' + colored('skipped', 'yellow')+': '+colored(str(total_submissions_added), 'green') \
                      +'/'+colored(str(total_submissions_skipped), 'yellow')+' subs, ' + colored(str(total_comments_added), 'green') \
                      + '/' + colored(str(total_comments_skipped), 'yellow')+' coms, '+colored(str(total_bonus_submissions), 'green')+' bonus; ' + elapsed_time_string + ' elapsed...')
      mongo = self.m.db.posts.find_one({'id': post.id})
      if mongo == None:
        data = {  'id': post.id,
                  'type': 'submission',
                  'author': str(post.author).lower(),
                  'created_utc': post.created_utc,
                  'title': post.title,
                  'subreddit': '%s'%post.subreddit,
                  'domain': post.domain,
                  'selftext': post.selftext,
                  'ups': post.ups,
                  'downs': post.downs,
                  'gilded': post.gilded,
                  'num_comments': post.num_comments
                }
        mongo_id = self.m.db.posts.insert(data)
        if verbose: print_log('ok', self.name+'.add_redditor:submission', 'Added post ID ' + post.id + ' to the database.')
        total_submissions_added += 1
        #return mongo_id    # Later functionality: consider returning list of mongo_id's added
      else:
        if verbose: print_log('warning', self.name+'.add_redditor:submission', 'Post ID ' + post.id + ' already exists in the database.')
        total_submissions_skipped += 1
        #return mongo['_id']
        
    # Get comments --------------------------------------
    try:
      comments = u.get_comments(limit=None)
    except Exception as e:
      print_log('error', self.name + '.add_redditor.get_comments', str(e))
      return None
    for post in comments:
      if not verbose:
        elapsed_time = time.time()-start_time
        m, s = divmod(elapsed_time, 60)
        h, m = divmod(m, 60)
        elapsed_time_string = '%d:%02d:%02d' % (h, m, s)
        sys.stdout.flush()
        sys.stdout.write('\r'+' '*80)
        sys.stdout.write('\r\t' + colored('Added', 'green') + '/' + colored('skipped', 'yellow')+': '+colored(str(total_submissions_added), 'green') \
                      +'/'+colored(str(total_submissions_skipped), 'yellow')+' subs, ' + colored(str(total_comments_added), 'green') \
                      + '/' + colored(str(total_comments_skipped), 'yellow')+' coms, '+colored(str(total_bonus_submissions), 'green')+' bonus; ' + elapsed_time_string + ' elapsed...')
      mongo = self.m.db.posts.find_one({'id': post.id})
      if mongo == None:
        data = {  'id': post.id,
                  'type': 'comment',
                  'author': str(post.author).lower(),
                  'created_utc': post.created_utc,
                  'subreddit': '%s'%post.subreddit,
                  'body': post.body,
                  'ups': post.ups,
                  'downs': post.downs,
                  'gilded': post.gilded,
                }
        mongo_id = self.m.db.posts.insert(data)
        total_comments_added += 1        
        result = re.search(r'http://www.reddit.com/r/(\w+)/comments/(\w+)/' , post.link_url)
        if result == None: continue
        submission_id = result.group(2)
        comment_submission_ids.append([mongo_id, submission_id])
        if verbose: print_log('ok', self.name+'.add_redditor:comment', 'Added post ID ' + post.id + ' to the database.')
        #return mongo_id    # Later functionality: consider returning list of mongo_id's added
      else:
        if verbose: print_log('warning', self.name+'.add_redditor:comment', 'Post ID ' + post.id + ' already exists in the database.')
        total_comments_skipped += 1
        #return mongo['_id']
        
    # Now add all comments' parent threads' OP's
    if len(comment_submission_ids) > 0:
      api_url = 'http://www.reddit.com/by_id/'
      for ids in comment_submission_ids:
        api_url += 't3_'+ids[1]+','
      i = 0
      for post in self.r.get_content(api_url, limit=None):
        if not verbose:
          elapsed_time = time.time()-start_time
          m, s = divmod(elapsed_time, 60)
          h, m = divmod(m, 60)
          elapsed_time_string = '%d:%02d:%02d' % (h, m, s)        
          sys.stdout.flush()
          sys.stdout.write('\r'+' '*80)
          sys.stdout.write('\r\t' + colored('Added', 'green') + '/' + colored('skipped', 'yellow')+': '+colored(str(total_submissions_added), 'green') \
                        +'/'+colored(str(total_submissions_skipped), 'yellow')+' subs, ' + colored(str(total_comments_added), 'green') \
                        + '/' + colored(str(total_comments_skipped), 'yellow')+' coms, '+colored(str(total_bonus_submissions), 'green')+' bonus; ' + elapsed_time_string + ' elapsed...')
        mongo = self.m.db.posts.find_one({'id': post.id})
        if mongo == None:
          data = {  'id': post.id,
                    'type': 'submission',
                    'author': str(post.author).lower(),
                    'created_utc': post.created_utc,
                    'title': post.title,
                    'subreddit': '%s'%post.subreddit,
                    'domain': post.domain,
                    'selftext': post.selftext,
                    'ups': post.ups,
                    'downs': post.downs,
                    'gilded': post.gilded,
                    'num_comments': post.num_comments
                  }
          mongo_id = self.m.db.posts.insert(data)
          self.m.db.posts.update({'_id':comment_submission_ids[i][0]}, {'$set': {'submission_mongo_id': mongo_id}})
          i += 1
          if verbose: print_log('ok', self.name+'.add_redditor:bonus submission', 'Added post ID ' + post.id + ' to the database.')
          total_bonus_submissions += 1
          #return mongo_id    # Later functionality: consider returning list of mongo_id's added
        else:
          if verbose: print_log('warning', self.name+'.add_redditor:bonus submission', 'Post ID ' + post.id + ' already exists in the database.')
          # ------------Code goes here to insert link to existing post
          i+=1
          #return mongo['_id']

    elapsed_time = time.time()-start_time
    m, s = divmod(elapsed_time, 60)
    h, m = divmod(m, 60)
    elapsed_time_string = '%d:%02d:%02d' % (h, m, s)
    if verbose: color = 'cyan'
    else: color = 'green'
    sys.stdout.write('\r'+' '*80)
    sys.stdout.write('\r\t' + colored('Added', 'green') + '/' + colored('skipped', 'yellow')+': '+colored(str(total_submissions_added), 'green') \
                  +'/'+colored(str(total_submissions_skipped), 'yellow')+' subs, ' + colored(str(total_comments_added), 'green') \
                  + '/' + colored(str(total_comments_skipped), 'yellow')+' coms, '+colored(str(total_bonus_submissions), 'green')+' bonus; ' + elapsed_time_string + ' elapsed...Finished.\n')
    if total_submissions_added>0 or total_comments_added>0:   # If we did something, add to the username database too
      if self.m.db.users.find_one({'username':username}) == None:
        self.m.db.users.insert({'username':username})
        # IDEA: also include the time, so we can keep track of updating account info
    return 'Added '+username+': '+str(total_submissions_added)+'/'+str(total_submissions_skipped)+' submissions and '+str(total_comments_added)+'/'+str(total_comments_skipped)+' comments.'
  def remove_redditor(self, username):
    """Removes all posts that are authored by a particular user.  Note that this does not remove submissions by other users which were added because they contained a comment of the specified user."""
    count = self.m.db.posts.find({'author':username}).count()
    self.m.db.posts.remove({'author':username})
    self.m.db.users.remove({'username':username})
    print_log('ok', self.name+'.remove_redditor:', 'Removed '+str(count)+' entries authored by '+username+'.')

  def remove_duplicates(self, username = None):
    """Removes duplicate posts in the database."""
    i = 0
    completed_posts = []
    if username == None: users = self.m.db.posts.distinct('author') # If no user is specified, look at all users
    else: users = [username]
    for u in users:                               # Iterate over users
      posts = self.m.db.posts.find({'author':u}, {'_id':True, 'id':True})  # Iterate over user's posts
      for p in posts:
        if p['id'] not in completed_posts:
          j = self.m.db.posts.remove( {'id':p['id'], '_id':{'$ne':p['_id']}})['n']
          if j > 0:
            completed_posts.append(p['id'])
            i += j
    return i
    
  def database_stats(self):
    """Prints some basic and broad statistics about the database."""
    num_submissions = self.m.db.posts.find({'type': 'submission'}).count()
    num_comments = self.m.db.posts.find({'type': 'comment'}).count()
    return 'RedditDB contains '+'{:,d}'.format(num_submissions)+' submissions and '+'{:,d}'.format(num_comments)+' comments in total.\n\n' \
            +'It contains data from ' + '{:,d}'.format(len(self.m.db.posts.distinct('author')))+' users, which includes '+'{:,d}'.format(self.m.db.users.count())+' users\' complete histories.'
    
    