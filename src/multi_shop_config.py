"""
Multi-Shop Configuration Manager for Fullbay API Ingestion

Handles multiple shops with different API keys and provides
shop selection and management capabilities.
"""

import os
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ShopConfig:
    """Configuration for a single shop."""
    shop_id: str
    shop_name: str
    api_key: str
    
    def __str__(self) -> str:
        return f"{self.shop_id}: {self.shop_name} (Key: {self.api_key[:8]}...{self.api_key[-4:]})"

class MultiShopConfigManager:
    """
    Manager for multiple shop configurations.
    
    Loads shop configurations from environment variables and provides
    methods to select and manage different shops.
    """
    
    def __init__(self):
        """Initialize the multi-shop configuration manager."""
        self.shops: Dict[str, ShopConfig] = {}
        self._load_shop_configs()
    
    def _load_shop_configs(self):
        """Load shop configurations from environment variables."""
        logger.info("Loading shop configurations...")
        
        # Look for shop configuration patterns: FULLBAY_API_KEY_SHOPID
        for env_var, value in os.environ.items():
            if env_var.startswith('FULLBAY_API_KEY_') and not value.endswith('_here'):
                # Extract shop ID from environment variable name
                shop_id = env_var.replace('FULLBAY_API_KEY_', '')
                
                # Get corresponding shop name
                shop_name_var = f'FULLBAY_SHOP_NAME_{shop_id}'
                shop_name = os.getenv(shop_name_var, f'Shop {shop_id}')
                
                # Create shop configuration
                shop_config = ShopConfig(
                    shop_id=shop_id,
                    shop_name=shop_name,
                    api_key=value
                )
                
                self.shops[shop_id] = shop_config
                logger.info(f"Loaded shop configuration: {shop_config}")
        
        if not self.shops:
            logger.warning("No shop configurations found!")
            logger.warning("Make sure you have FULLBAY_API_KEY_SHOPID variables set")
    
    def get_shop_ids(self) -> List[str]:
        """Get list of available shop IDs."""
        return list(self.shops.keys())
    
    def get_shop_config(self, shop_id: str) -> Optional[ShopConfig]:
        """Get configuration for a specific shop."""
        return self.shops.get(shop_id)
    
    def get_all_shops(self) -> Dict[str, ShopConfig]:
        """Get all shop configurations."""
        return self.shops.copy()
    
    def list_shops(self) -> None:
        """Print a list of all available shops."""
        if not self.shops:
            print("❌ No shops configured!")
            return
        
        print("📋 Available Shops:")
        print("-" * 40)
        for shop_id, config in self.shops.items():
            print(f"  {shop_id}: {config.shop_name}")
            print(f"    API Key: {config.api_key[:8]}...{config.api_key[-4:]}")
        print("-" * 40)
    
    def validate_shop_config(self, shop_id: str) -> Tuple[bool, str]:
        """
        Validate a shop configuration.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if shop_id not in self.shops:
            return False, f"Shop '{shop_id}' not found"
        
        config = self.shops[shop_id]
        
        if not config.api_key or config.api_key.endswith('_here'):
            return False, f"Shop '{shop_id}' has invalid API key"
        
        if len(config.api_key) < 10:  # Basic length check
            return False, f"Shop '{shop_id}' API key appears too short"
        
        return True, "Valid"
    
    def interactive_shop_selection(self) -> Optional[str]:
        """
        Interactive shop selection for command-line use.
        
        Returns:
            Selected shop ID or None if cancelled
        """
        if not self.shops:
            print("❌ No shops configured!")
            return None
        
        print("\n🏪 SELECT SHOP FOR INGESTION")
        print("=" * 40)
        
        shop_list = list(self.shops.items())
        
        for i, (shop_id, config) in enumerate(shop_list, 1):
            print(f"  {i}. {config.shop_name} ({shop_id})")
        
        print(f"  0. Cancel")
        print("-" * 40)
        
        while True:
            try:
                choice = input("Enter your choice (number): ").strip()
                
                if choice == '0':
                    print("❌ Operation cancelled")
                    return None
                
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(shop_list):
                    selected_shop_id = shop_list[choice_num - 1][0]
                    selected_config = self.shops[selected_shop_id]
                    
                    print(f"✅ Selected: {selected_config.shop_name} ({selected_shop_id})")
                    return selected_shop_id
                else:
                    print(f"❌ Please enter a number between 0 and {len(shop_list)}")
                    
            except ValueError:
                print("❌ Please enter a valid number")
            except KeyboardInterrupt:
                print("\n❌ Operation cancelled")
                return None
    
    def add_shop_to_config_file(self, shop_id: str, shop_name: str, api_key: str) -> bool:
        """
        Add a new shop to the local configuration file.
        
        Args:
            shop_id: Shop identifier (e.g., 'CHI', 'NYC')
            shop_name: Human-readable shop name
            api_key: Fullbay API key for this shop
            
        Returns:
            True if successful, False otherwise
        """
        try:
            config_file = "local_config.env"
            
            # Read existing config
            lines = []
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    lines = f.readlines()
            
            # Find the insertion point (after the last shop or after the comment block)
            insert_index = len(lines)
            for i, line in enumerate(lines):
                if line.strip().startswith('# Default API Configuration'):
                    insert_index = i
                    break
            
            # Create new shop configuration lines
            new_lines = [
                f"# {shop_name}\n",
                f"FULLBAY_API_KEY_{shop_id}={api_key}\n",
                f"FULLBAY_SHOP_NAME_{shop_id}={shop_name}\n",
                "\n"
            ]
            
            # Insert the new configuration
            for i, new_line in enumerate(new_lines):
                lines.insert(insert_index + i, new_line)
            
            # Write back to file
            with open(config_file, 'w') as f:
                f.writelines(lines)
            
            # Reload configurations
            os.environ[f'FULLBAY_API_KEY_{shop_id}'] = api_key
            os.environ[f'FULLBAY_SHOP_NAME_{shop_id}'] = shop_name
            self._load_shop_configs()
            
            logger.info(f"Added shop configuration: {shop_id} - {shop_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add shop configuration: {e}")
            return False

def main():
    """Test the multi-shop configuration manager."""
    print("🏪 MULTI-SHOP CONFIGURATION MANAGER TEST")
    print("=" * 50)
    
    # Load environment variables from local config
    def load_local_env():
        env_file = "local_config.env"
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if not value.endswith('_here'):
                            os.environ[key] = value
    
    load_local_env()
    
    # Initialize manager
    manager = MultiShopConfigManager()
    
    # List shops
    manager.list_shops()
    
    # Validate configurations
    print("\n🔍 CONFIGURATION VALIDATION")
    print("-" * 30)
    for shop_id in manager.get_shop_ids():
        is_valid, message = manager.validate_shop_config(shop_id)
        status = "✅" if is_valid else "❌"
        print(f"  {status} {shop_id}: {message}")

if __name__ == "__main__":
    main()
