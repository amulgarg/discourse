class IgnoredUser < ActiveRecord::Base
  belongs_to :user
  belongs_to :ignored_user, class_name: "User"
end

# == Schema Information
#
# Table name: ignored_users
#
#  id            :integer          not null, primary key
#  user_id       :integer          not null
#  ignored_user_id :integer          not null
#  created_at    :datetime         not null
#  updated_at    :datetime         not null
#
# Indexes
#
#  index_ignored_users_on_ignored_user_id_and_user_id  (ignored_user_id,user_id) UNIQUE
#  index_ignored_users_on_user_id_and_ignored_user_id  (user_id,ignored_user_id) UNIQUE
#
